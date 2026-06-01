# Sistema de Recuperação de Informação — Sports Odds Crawler

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura do Sistema](#2-arquitetura-do-sistema)
3. [Fase 1 — Coleta (Crawler)](#3-fase-1--coleta-crawler)
   - 3.1 [Engine Assíncrona](#31-engine-assíncrona)
   - 3.2 [Scheduler Distribuído com Redis](#32-scheduler-distribuído-com-redis)
   - 3.3 [Respeito ao robots.txt](#33-respeito-ao-robotstxt)
   - 3.4 [Extrator de Links com Prioridade](#34-extrator-de-links-com-prioridade)
   - 3.5 [Dashboard de Monitoramento Web](#35-dashboard-de-monitoramento-web)
4. [Fase 2 — Indexação](#4-fase-2--indexação)
   - 4.1 [Pipeline de Processamento](#41-pipeline-de-processamento)
   - 4.2 [Limpeza de HTML](#42-limpeza-de-html)
   - 4.3 [Detecção de Idioma](#43-detecção-de-idioma)
   - 4.4 [Tokenização e Stemming](#44-tokenização-e-stemming)
   - 4.5 [Chunking](#45-chunking)
   - 4.6 [Índice Invertido no Redis](#46-índice-invertido-no-redis)
   - 4.7 [Métricas de Indexação](#47-métricas-de-indexação)
5. [Fase 3 — Recuperação e Ranking](#5-fase-3--recuperação-e-ranking)
   - 5.1 [Algoritmo BM25](#51-algoritmo-bm25)
   - 5.2 [Modos de Busca AND / OR com Fallback](#52-modos-de-busca-and--or-com-fallback)
   - 5.3 [Interface Web de Busca](#53-interface-web-de-busca)
   - 5.4 [REPL Interativo (Terminal)](#54-repl-interativo-terminal)
   - 5.5 [CLI de Busca](#55-cli-de-busca)
6. [Estrutura de Dados no Redis](#6-estrutura-de-dados-no-redis)
7. [Decisões de Projeto e Trade-offs](#7-decisões-de-projeto-e-trade-offs)
8. [Bibliotecas Externas](#8-bibliotecas-externas)
9. [Métricas de Desempenho](#9-métricas-de-desempenho)
10. [Como Executar](#10-como-executar)

---

## 1. Visão Geral

O **Sports Odds Crawler** é um sistema completo de recuperação de informação voltado ao domínio de apostas esportivas. Ele cobre as três etapas clássicas de um sistema de IR:

| Etapa | Responsabilidade | Módulo principal |
|---|---|---|
| **Coleta** | Rastrear páginas web de odds esportivas | `src/crawler/` |
| **Indexação** | Construir índice invertido com stemming | `src/indexer/` |
| **Recuperação** | Busca com ranking BM25 e interface web | `src/indexer/server.py`, `search.py` |

O sistema foi projetado para escalabilidade horizontal: múltiplas instâncias do crawler podem operar em paralelo compartilhando o mesmo Redis, e o índice é persistido no Redis para consultas em tempo real.

**Domínio de dados:** páginas de sites de odds esportivas (OddsPortal, BMBets, OddsAgora, Forebet, BetExplorer, SoccerVital, TipsScore), cobrindo esportes como basquete, tênis, futebol americano, hóquei no gelo, vôlei, e-sports e handebol.

---

## 2. Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FASE 1 — COLETA                            │
│                                                                     │
│  Seeds (URLs)                                                       │
│       │                                                             │
│       ▼                                                             │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │               CrawlerEngine (anyio + httpx)                 │   │
│  │   Worker 0  Worker 1  ...  Worker N  (até 100 simultâneos)  │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                             │                                       │
│           ┌─────────────────┼──────────────────┐                   │
│           ▼                 ▼                  ▼                   │
│    RedisScheduler      RobotsCache        LinkExtractor             │
│    (fila + dedup)    (robots.txt)       (prioridade)                │
│           │                                                         │
│           ▼                                                         │
│      MongoDB  ◄──── RawWebDocument {url, html, timestamp, ...}     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼  (odds_data collection)
┌─────────────────────────────────────────────────────────────────────┐
│                        FASE 2 — INDEXAÇÃO                          │
│                                                                     │
│  MongoDB / corpus.zst                                               │
│       │                                                             │
│       ▼                                                             │
│  clean_html()  →  detect_lang()  →  tokenize_and_stem()            │
│  (BeautifulSoup)  (langdetect)    (NLTK RSLP / Porter)             │
│       │                                                             │
│       ▼                                                             │
│  chunk_tokens()  →  index_doc()                                     │
│  (janelas de N)    (pipeline Redis)                                 │
│                          │                                          │
│                          ▼                                          │
│                       Redis                                         │
│               idx:term:{stem}  →  {chunk_id, ...}                  │
│               chunk:{id}       →  {url, lang, stems, raw}          │
│               idx:total_chunks →  N                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      FASE 3 — RECUPERAÇÃO                          │
│                                                                     │
│  Consulta do usuário                                                │
│       │                                                             │
│       ├── detect_lang()  →  tokenize_and_stem()                    │
│       │                                                             │
│       ├─ modo AND: SINTER(idx:term:*) ─┐                           │
│       └─ modo OR:  SMEMBERS union    ──┼→ chunk_ids candidatos     │
│                                        │                            │
│                                        ▼                            │
│                              bm25_score()                           │
│                         (TF-IDF normalizado)                        │
│                                        │                            │
│                                        ▼                            │
│              ┌─────────────────────────────────────────┐           │
│              │  Interface Web  │  REPL  │  CLI          │           │
│              │  localhost:8888 │  repl  │  search       │           │
│              └─────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Fase 1 — Coleta (Crawler)

### 3.1 Engine Assíncrona

**Arquivo:** `src/crawler/engine.py`

O núcleo do crawler é a classe `CrawlerEngine`, que gerencia um pool de workers assíncronos usando a biblioteca **anyio** (compatível com asyncio e trio). Cada worker executa o seguinte ciclo:

```
aguarda URL na fila  →  verifica robots.txt  →  faz GET HTTP  →
extrai links  →  salva documento  →  repete
```

**Concorrência por domínio:** para evitar sobrecarga em servidores individuais, cada domínio possui um `CapacityLimiter` (`anyio`) que limita o número de requisições simultâneas ao mesmo host (configurável via `MAX_CONCURRENCY_PER_DOMAIN`, padrão 8).

**Cliente HTTP:** `httpx.AsyncClient` com suporte a HTTP/1.1, connection pooling (`MAX_CONNECTIONS = 200`, `MAX_KEEPALIVE = 200`) e timeout configurável. O User-Agent simula um navegador Chrome para maximizar a taxa de sucesso.

**Parada global:** o worker que atingir `MAX_PAGES` registra um sinal de parada (`crawler:stop`) no Redis. Todos os outros workers verificam essa chave antes de processar cada URL, permitindo parada coordenada entre múltiplas instâncias.

### 3.2 Scheduler Distribuído com Redis

**Arquivo:** `src/scheduler/redis_scheduler.py`

A fila de URLs é implementada como um **Sorted Set** no Redis:

| Chave Redis | Tipo | Conteúdo |
|---|---|---|
| `crawler:queue` | Sorted Set | URL → score (prioridade × 1000 + profundidade) |
| `crawler:seen` | Set | Todas as URLs já enfileiradas (deduplicação) |
| `crawler:busy` | String | Contador de workers ativos |
| `crawler:total` | String | Total de páginas coletadas (global) |
| `crawler:stop` | String | Flag de parada |

**Deduplicação atômica:** o comando Redis `SADD` é atômico — retorna `1` se o elemento foi inserido (URL nova) e `0` se já existia. Isso garante que mesmo com múltiplos processos rodando em paralelo, cada URL seja processada exatamente uma vez, sem race conditions.

**Dequeue bloqueante:** `BZPOPMIN` bloqueia até 1 segundo aguardando a próxima URL de maior prioridade (menor score). Isso evita busy-polling e permite que workers durmam eficientemente quando a fila está vazia.

**Profundidade no score:** o score é codificado como `prioridade × 1000 + depth`. No dequeue, `depth = score % 1000`. Isso permite recuperar a profundidade sem armazenar metadados separados.

**Escalabilidade horizontal:** múltiplos processos (ou containers Docker) apontam para o mesmo Redis. O `BZPOPMIN` garante que apenas um worker consuma cada URL. Não há coordenação extra necessária.

### 3.3 Respeito ao robots.txt

**Arquivo:** `src/crawler/robots.py`

A classe `RobotsCache` implementa cache por domínio das regras de `robots.txt`:

- Cada domínio tem seu próprio `asyncio.Lock`, evitando múltiplas requisições ao mesmo `robots.txt` em paralelo
- Falhas de fetch (timeout, 404, etc.) resultam em permissão total (fail-open)
- O cache persiste durante toda a sessão do crawler (em memória)
- Utiliza `urllib.robotparser.RobotFileParser` da biblioteca padrão do Python

**Decisão:** adotamos fail-open (permitir acesso quando o robots.txt não pode ser obtido) para maximizar a cobertura do corpus, aceitando o risco de coletar algumas páginas que poderiam ser bloqueadas.

### 3.4 Extrator de Links com Prioridade

**Arquivo:** `src/parser/link_extractor.py`

O extrator usa **regex** (`href=["'][^"']+["']`) ao invés de parser HTML completo — decisão deliberada para maximizar throughput (até 10× mais rápido que BeautifulSoup para extração de links pura).

**Filtragens aplicadas:**
- URLs com esquemas não-HTTP (`javascript:`, `mailto:`, `tel:`, `#`)
- Extensões de arquivos não-HTML (`.png`, `.pdf`, `.js`, `.css`, etc.)
- Domínios fora da lista de sementes

**Normalização:** remoção de fragmentos (`#`), remoção de barra final em URLs não-raiz.

**Sistema de prioridades** (score menor = maior prioridade no Sorted Set):

| Condição na URL | Ajuste de prioridade | Justificativa |
|---|---|---|
| Contém `live`, `today`, `recent` | −5 | Odds ao vivo são mais relevantes |
| Contém `basketball`, `tennis`, etc. | −3 | Esportes prioritários |
| Contém `soccer`, `football` | +5 | De-priorizar futebol (cobertura ampla demais) |
| Padrão | 0 (base 10) | — |

### 3.5 Dashboard de Monitoramento Web

**Arquivo:** `src/monitor/server.py` — `http://localhost:8080`

Dashboard web em tempo real usando **Server-Sent Events (SSE)**, atualizado a cada segundo. Implementado com `aiohttp` (sem frameworks pesados).

**Métricas exibidas:**
- Páginas coletadas, erros, taxa de sucesso, workers ativos, tamanho da fila, URLs vistas
- Barra de progresso até `MAX_PAGES`
- Gráfico de throughput (páginas/segundo, últimos 120 segundos) via Chart.js
- Breakdown de erros por tipo com barras proporcionais
- Feed de atividade recente (URLs coletadas)
- Terminal de logs em tempo real com filtro e auto-scroll

**Decisão de arquitetura SSE vs WebSocket:** SSE é unidirecional (servidor → cliente), mais simples de implementar e suficiente para dashboards de monitoramento. WebSocket seria necessário apenas se o cliente precisasse enviar dados ao servidor.

---

## 4. Fase 2 — Indexação

### 4.1 Pipeline de Processamento

**Arquivo:** `src/indexer/main.py`

Para cada documento no corpus:

```
HTML bruto
    │
    ▼ clean_html()
Texto limpo
    │
    ▼ detect_lang()
Idioma detectado ("pt" | "en")
    │
    ▼ tokenize_and_stem()
Lista de stems  [s₁, s₂, ..., sₙ]
    │
    ▼ chunk_tokens()
Chunks [(chunk_id, stems), ...]    +    raw_chunks {chunk_id: texto_original}
    │
    ▼ index_doc()
Redis ← posting lists + metadados
```

### 4.2 Limpeza de HTML

**Arquivo:** `src/indexer/cleaner.py`

Usa `BeautifulSoup` com parser `lxml` (o mais rápido disponível). Etapas:

1. **Remoção de ruído:** tags `<script>`, `<style>`, `<nav>`, `<footer>`, `<header>`, `<aside>`, `<noscript>`, `<form>`, `<iframe>`, `<svg>` são completamente removidas do DOM antes da extração de texto.

2. **Seleção de conteúdo:** tenta usar `<main>` primeiro, depois `<article>`, depois `<body>`, e por último o documento inteiro — em ordem de especificidade semântica.

3. **Normalização de espaço:** regex `\s+` → `" "` elimina múltiplos espaços e quebras de linha.

**Vantagem sobre regex puro:** o parser DOM garante que conteúdo dentro de tags removidas não vaze para o texto (ao contrário de uma remoção por regex que pode deixar restos).

**Desvantagem:** mais lento que regex para extração simples. Mitigado pelo uso do parser `lxml` (C nativo) ao invés do parser HTML puro do Python.

### 4.3 Detecção de Idioma

**Arquivo:** `src/indexer/lang.py`

Dois mecanismos em cascata:

1. **Heurística por domínio:** o domínio `oddsagora.com.br` é forçado como `"pt"` antes mesmo de analisar o texto. Decisão baseada no conhecimento prévio do corpus.

2. **Detecção automática via `langdetect`:** usa os primeiros 2000 caracteres do texto limpo. A biblioteca implementa o algoritmo de n-gramas de caracteres do Google. A semente (`DetectorFactory.seed = 0`) torna o resultado determinístico.

3. **Fallback:** em caso de exceção (texto muito curto, caracteres não-ASCII, etc.), assume `"en"`.

**Resultado binário:** o sistema trata apenas dois idiomas — `"pt"` (Português) e `"en"` (Inglês) — pois o corpus é exclusivamente desses dois idiomas.

### 4.4 Tokenização e Stemming

**Arquivo:** `src/indexer/nlp.py`

**Tokenização:** `nltk.tokenize.word_tokenize()` com o modelo de linguagem específico (`portuguese` ou `english`). O word_tokenize usa o **Punkt Tokenizer** (modelo estatístico treinado) que trata corretamente pontuação, contrações e abreviações.

**Filtros aplicados:**
- Regex `^[a-záéíóúâêôãõàç]{2,}$` — mantém apenas tokens alfabéticos com acento e comprimento ≥ 2 caracteres. Remove números, pontuação e tokens mistos (ex: "3x").
- **Stopwords NLTK:** listas pré-definidas para português e inglês, com exceção de termos do domínio.

**Termos de domínio preservados** (`DOMAIN_KEEP`):
```
over, under, handicap, gol, escanteio, odd, odds, draw, empate, corner, set, match
```
Esses termos são removidos das listas de stopwords porque são altamente relevantes no contexto de apostas esportivas. Sem isso, uma busca por "over" não encontraria nenhum resultado.

**Stemming:** dois algoritmos, selecionados por idioma:

| Idioma | Algoritmo | Biblioteca | Exemplo |
|---|---|---|---|
| Português | **RSLP** (Removedor de Sufixos da Língua Portuguesa) | NLTK | "apostadores" → "apost" |
| Inglês | **Porter Stemmer** | NLTK | "basketball" → "basketbal" |

O **RSLP** foi desenvolvido especificamente para o português por Viviane Orengo e Christian Huyck. Aplica regras de remoção de sufixos em 8 passos: plural, feminino, argumentativo, aumentativo, diminutivo, advérbio, substantivo e verbal.

O **Porter Stemmer** é o algoritmo clássico para inglês (1980), amplamente adotado em sistemas de IR. Aplica 5 fases de remoção de sufixos via regras morfológicas.

**Vantagem do stemming sobre lemmatização:** mais rápido (regras vs. dicionário) e sem necessidade de tagger morfossintático. A perda de precisão (stems não são palavras reais) é aceitável pois tanto documento quanto consulta passam pelo mesmo processo.

### 4.5 Chunking

**Arquivo:** `src/indexer/chunker.py`

```python
def chunk_tokens(stems, doc_id, n):
    return [(f"{doc_id}:{i // n}", stems[i : i + n])
            for i in range(0, len(stems), n)]
```

Cada documento é dividido em **janelas fixas de `n` stems** (padrão: `n = 100`). O `chunk_id` é `{doc_id}:{chunk_index}` (ex: `"69da97d0:5"`).

**Por que chunking?**
- Documentos HTML são grandes (dezenas de KB). Indexar o documento inteiro como unidade produz posting lists enormes e pontuação BM25 diluída.
- Chunks menores permitem retornar trechos específicos relevantes, não o documento inteiro.
- Chunks de tamanho fixo garantem que o comprimento médio (`avgdl`) seja estável para o BM25.

**Texto original por chunk:** em `main.py`, uma janela proporcional de palavras originais é calculada para cada chunk:

```python
ratio = len(words) / max(len(stems), 1)
w_start = int(chunk_index * chunk_size * ratio)
w_end   = int((chunk_index + 1) * chunk_size * ratio)
raw_text = " ".join(words[w_start:w_end])
```

Essa aproximação usa a proporção `words/stems` para mapear posições de stems de volta ao texto original. É uma heurística — stopwords e tokens filtrados introduzem um pequeno deslocamento — mas é suficiente para snippets legíveis.

**Comparação de chunk sizes** (experimento realizado):

| Chunk Size | Chunks | Termos | Tamanho Índice | avg posting length |
|---|---|---|---|---|
| 50 | ~22.000 | ~8.000 | ~65 MB | ~62 |
| 100 | 11.123 | 8.052 | 65.9 MB | 125.94 |
| 200 | ~5.500 | ~7.800 | ~64 MB | ~250 |
| 500 | ~2.200 | ~7.500 | ~62 MB | ~620 |

Chunk size 100 apresentou o melhor equilíbrio entre granularidade de resultado e tamanho do índice.

### 4.6 Índice Invertido no Redis

**Arquivo:** `src/indexer/inverted_index.py`

O índice é armazenado integralmente em memória no Redis, usando dois tipos de estruturas:

**Posting lists** (Sets Redis):
```
idx:term:{stem}  →  { chunk_id_1, chunk_id_2, ... }
```
Exemplo: `idx:term:basketbal` → `{ "69da97d0:3", "69da97d0:7", "69da97fa:1", ... }`

Sets são usados (ao invés de Sorted Sets ou Listas) porque permitem operações de conjunto — `SINTER` (AND) e `SUNION` (OR) — em O(N) com N = tamanho do resultado, sem necessidade de iteração em Python.

**Metadados de chunk** (Hashes Redis):
```
chunk:{chunk_id}  →  {
    doc_url:  "https://...",
    lang:     "en",
    stems:    "basketbal odd bet ...",   ← para BM25 TF
    text:     "basketbal odd bet ..."    ← alias backwards-compat
    raw:      "basketball odds betting", ← texto original legível
    offset:   "5"                        ← índice do chunk no doc
}
```

**Escrita em pipeline:** todas as operações de um documento são agrupadas em um único pipeline Redis (sem transação, para máximo throughput). Pipelines são "liberados" a cada 2000 operações acumuladas para limitar o uso de memória do buffer.

**Contadores globais:**
```
idx:total_chunks  →  N    (total de chunks indexados, para IDF)
idx:chunk_size    →  100  (parâmetro avgdl para BM25)
```

**Export do índice:** `src/indexer/export_index.py` exporta o índice para `JSONL` (um objeto por linha), contendo tanto os posting lists quanto os metadados de cada chunk. Usado para backup e restauração sem MongoDB.

### 4.7 Métricas de Indexação

**Arquivo:** `src/indexer/metrics.py`

Coleta automática ao final de cada run de indexação:

| Métrica | Valor (chunk_size=100) |
|---|---|
| Documentos processados | 435 |
| Chunks indexados | 11.123 |
| Tokens totais | 1.087.902 |
| Tempo total | 119.4 s |
| Throughput | 3.64 docs/s · 93.16 chunks/s · 9.111 tokens/s |
| Termos únicos no índice | 8.052 |
| Média de postings por termo | 125.94 chunks |
| Tamanho do índice no Redis | 65.9 MB |

---

## 5. Fase 3 — Recuperação e Ranking

### 5.1 Algoritmo BM25

**Arquivo:** `src/indexer/ranking.py`

BM25 (**Best Match 25**) é o modelo probabilístico de recuperação de informação mais adotado na indústria, sendo o padrão do Elasticsearch e Solr. Supera o TF-IDF clássico por aplicar **saturação de frequência** e **normalização por comprimento do documento**.

#### Fórmula completa

Para uma consulta $Q = \{q_1, q_2, \ldots, q_m\}$ e um chunk $d$:

$$\text{BM25}(Q, d) = \sum_{i=1}^{m} \text{IDF}(q_i) \cdot \frac{f(q_i, d) \cdot (k_1 + 1)}{f(q_i, d) + k_1 \cdot \left(1 - b + b \cdot \dfrac{|d|}{\text{avgdl}}\right)}$$

#### Componentes

**IDF (Inverse Document Frequency):** penaliza termos que aparecem em muitos documentos:

$$\text{IDF}(q_i) = \ln\left(\frac{N - \text{df}(q_i) + 0.5}{\text{df}(q_i) + 0.5} + 1\right)$$

- $N$ = total de chunks indexados (lido de `idx:total_chunks` no Redis)
- $\text{df}(q_i)$ = número de chunks que contêm o termo $q_i$ (lido com `SCARD idx:term:{stem}`)
- O `+1` garante que o IDF seja sempre positivo, mesmo quando $\text{df} = N$

**TF saturado:** o numerador $f(q_i, d) \cdot (k_1 + 1)$ e o denominador crescem juntos à medida que TF aumenta, mas o denominador cresce mais devagar — isso produz saturação:

$$\lim_{f \to \infty} \frac{f \cdot (k_1 + 1)}{f + k_1 \cdot (\ldots)} = k_1 + 1$$

Um termo que aparece 100 vezes não recebe 100× mais peso do que um que aparece 1 vez. O parâmetro $k_1 = 1.5$ controla a velocidade de saturação.

**Normalização por comprimento** ($b = 0.75$): o fator $\left(1 - b + b \cdot \frac{|d|}{\text{avgdl}}\right)$ penaliza chunks mais longos que a média, evitando que documentos verbosos dominem os resultados:

- $b = 0$: sem normalização por comprimento
- $b = 1$: normalização completa
- $b = 0.75$: valor empírico amplamente adotado na literatura

#### Implementação

```python
K1 = 1.5
B  = 0.75

async def bm25_score(redis, query_stems, chunk_ids, *, avgdl=100.0):
    # 1. Obter DF de cada termo via pipeline Redis
    pipe = redis.pipeline(transaction=False)
    for stem in query_stems:
        pipe.scard(f"idx:term:{stem}")
    dfs = [max(int(v), 1) for v in await pipe.execute()]

    # 2. Total de chunks (N) do contador global
    N = int(await redis.get("idx:total_chunks") or 1)

    # 3. Obter metadados de cada chunk candidato
    pipe = redis.pipeline(transaction=False)
    for cid in chunk_ids:
        pipe.hgetall(f"chunk:{cid}")
    metas = await pipe.execute()

    # 4. Calcular BM25 para cada chunk
    scored = []
    for cid, meta in zip(chunk_ids, metas):
        stem_list = meta.get("stems", "").split()
        doc_len   = len(stem_list) or avgdl
        score     = 0.0
        for stem, df in zip(query_stems, dfs):
            tf  = stem_list.count(stem)                      # TF
            idf = math.log((N - df + 0.5) / (df + 0.5) + 1) # IDF
            num = tf * (K1 + 1)
            den = tf + K1 * (1 - B + B * doc_len / avgdl)
            score += idf * (num / max(den, 1e-9))
        scored.append((cid, score, meta))

    return sorted(scored, key=lambda x: x[1], reverse=True)
```

**Por que BM25 e não TF-IDF clássico?**

| Característica | TF-IDF clássico | BM25 |
|---|---|---|
| Saturação de TF | Não — cresce linearmente | Sim — assintótico a $k_1 + 1$ |
| Normalização por comprimento | Parcial (pelo comprimento do vetor) | Explícita com parâmetro $b$ |
| Justificativa teórica | Ad-hoc | Modelo probabilístico de relevância |
| Desempenho empírico | Razoável | Superior em benchmarks TREC |

**Eficiência:** as operações de Redis são todas agrupadas em pipelines assíncronos, minimizando round-trips de rede. Para 100 chunks candidatos, o scoring completo exige apenas 3 round-trips ao Redis: um para DFs, um para o contador N, e um para os metadados.

### 5.2 Modos de Busca AND / OR com Fallback

**Arquivo:** `src/indexer/search.py`

**Processamento da consulta:**
1. Detectar idioma da consulta (mesma heurística da indexação)
2. Tokenizar e stemizar com NLTK
3. Construir chaves Redis: `[f"idx:term:{stem}" for stem in stems]`

**Modo AND (padrão):** usa `SINTER` do Redis — interseção dos posting lists:
```python
chunk_ids = await redis.sinter(*keys)
```
Retorna apenas chunks que contêm **todos** os termos da consulta. Alta precisão, menor revocação.

**Modo OR:** une os posting lists com `SMEMBERS` iterativo:
```python
all_ids = set()
for key in keys:
    all_ids.update(await redis.smembers(key))
```
Retorna chunks com **qualquer** um dos termos. Maior revocação, pontuação BM25 ordena por relevância.

**Fallback automático AND → OR:** se o modo AND retornar zero resultados (termos não co-ocorrem no mesmo chunk), o sistema automaticamente executa OR e sinaliza o modo efetivo ao usuário:
```
Query: "futebol americano touchdown"  [OR (FALLBACK)]  3 results
```

Isso resolve o problema de consultas com múltiplos termos específicos que podem estar em chunks diferentes do mesmo documento.

### 5.3 Interface Web de Busca

**Arquivo:** `src/indexer/server.py` — `http://localhost:8888`

Servidor web implementado com `aiohttp`, sem dependências de front-end (vanilla JS + CSS inline). Dois endpoints:

**`GET /`** — Serve a página HTML da interface de busca

**`GET /search?q={query}&mode={and|or}&k={n}`** — API JSON:
```json
{
  "results": [
    {
      "chunk_id": "69da97d0:32",
      "score": 6.4446,
      "lang": "en",
      "url": "https://www.bmbets.com/basketball/",
      "snippet": "Basketball odds betting comparison..."
    }
  ],
  "mode": "and",
  "query": "basketball odds"
}
```

**Interface do usuário:**

```
┌─────────────────────────────────────────────────────────────┐
│          ⚡ SPORTS ODDS SEARCH                              │
│     BM25 · Inverted Index · Redis · NLTK stemming           │
├─────────────────────────────────────────────────────────────┤
│ ┌────────────────────────────────────────┐ ┌──────────┐    │
│ │ basketball odds, futebol gol, ...      │ │  Buscar  │    │
│ └────────────────────────────────────────┘ └──────────┘    │
│  Modo: [AND — todos os termos ▾]   Resultados: [10]        │
├─────────────────────────────────────────────────────────────┤
│ 5 resultados · modo AND · 42 ms                             │
├─────────────────────────────────────────────────────────────┤
│ ┌───────────────────────────────────────────────────────┐  │
│ │ #1  score 6.444  [en]  https://bmbets.com/basketball/ │  │
│ │ Championship Nicky Rackard Cup Bowling Map World PBA   │  │
│ │ Armwrestling East vs West. Best of five East vs West   │  │
│ └───────────────────────────────────────────────────────┘  │
│ ┌───────────────────────────────────────────────────────┐  │
│ │ #2  score 5.910  [pt]  https://oddsagora.com.br        │  │
│ │ apostadores aproveitam estrutura OddsAgora obter       │  │
│ │ informações tempo real apostar melhores probabilidades  │  │
│ └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Funcionalidades:**
- Seletor de modo AND/OR
- Controle de número de resultados (1–50)
- Tempo de resposta exibido (round-trip client-server)
- Destaque dos termos da consulta nos snippets (highlight em roxo)
- Cards clicáveis com URL aberta em nova aba
- Mensagem de "nenhum resultado" com ícone
- Tema escuro consistente com o dashboard do crawler

**Decisão de não usar framework:** Flask, FastAPI e Django adicionariam dependências desnecessárias para um endpoint simples. `aiohttp` já está no projeto como dependência do crawler e oferece server HTTP assíncrono nativo.

### 5.4 REPL Interativo (Terminal)

**Arquivo:** `src/indexer/repl.py`

Interface de linha de comando interativa usando `rich.prompt.Prompt`:

```
  ╔═══════════════════════════════════════╗
  ║   Sports Odds Search  —  BM25 index  ║
  ╚═══════════════════════════════════════╝
  Comandos:
    <consulta>           busca em modo AND (todos os termos)
    or: <consulta>       busca em modo OR  (algum dos termos)
    exit / Ctrl-C        sair

busca> basketball odds
...
busca> or: futebol gol handicap
...
```

**Sintaxe `or:`:** prefixar a consulta com `or:` força o modo OR sem parâmetros de linha de comando. Facilita a comparação interativa entre os dois modos.

### 5.5 CLI de Busca

**Arquivo:** `src/indexer/search.py`

```bash
uv run search "basketball odds" -k 10
uv run search --mode or "futebol gol" -k 5
uv run search "tennis match set" --plain    # saída sem Rich (para scripts)
```

Saída formatada com `rich` (tabelas coloridas, scores destacados) ou saída plain para integração com outros scripts via `--plain`.

---

## 6. Estrutura de Dados no Redis

### Namespaces de chaves

```
idx:term:{stem}       Set    Posting list do termo
chunk:{id}            Hash   Metadados do chunk
doc:{id}:chunks       Set    Chunks pertencentes a um documento
idx:total_chunks      String Total de chunks (para BM25 N)
idx:chunk_size        String Tamanho dos chunks (para BM25 avgdl)

crawler:queue         ZSet   Fila de URLs (score = prioridade)
crawler:seen          Set    URLs já visitadas (dedup)
crawler:busy          String Contador de workers ativos
crawler:total         String Total de páginas coletadas
crawler:stop          String Flag de parada global
crawler:workers       Hash   Status por worker (pid:id → status)
crawler:activity      List   Feed de URLs recentes
crawler:logs          List   Log ring-buffer (últimas 500 entradas)
crawler:error_types   Hash   Contagem de erros por tipo
```

### Exemplo real de dados

```redis
> SMEMBERS idx:term:basketbal
1) "69da97d0:3"
2) "69da97d0:7"
3) "69da97fa:1"
...

> HGETALL chunk:69da97d0:3
1) "doc_url"
2) "https://www.bmbets.com/basketball/"
3) "lang"
4) "en"
5) "stems"
6) "basketbal bet odd compar odd plac bet bookmak benefit ..."
7) "raw"
8) "basketball betting odds comparison place bet bookmaker benefit ..."
9) "offset"
10) "3"

> GET idx:total_chunks
"3118"

> SCARD idx:term:fight
"293"
```

---

## 7. Decisões de Projeto e Trade-offs

### Redis como índice invertido

**Vantagem:**
- `SINTER` e `SUNION` nativos para operações AND/OR sem código Python
- Estruturas de dados ricas (Sets, Hashes, Sorted Sets, Listas) mapeiam diretamente para as necessidades do IR
- Acesso em memória: latência de consulta < 10ms para posting lists de centenas de chunks
- Persistência configurável (AOF) para durabilidade
- Escalabilidade horizontal futura via Redis Cluster

**Desvantagem:**
- Custo de memória: 65 MB para 11.000 chunks e 8.000 termos
- Não suporta buscas por prefixo ou fuzzy nativamente (requer módulo RediSearch)
- Sem suporte a scoring nativo — BM25 precisa ser implementado em Python

**Alternativas consideradas:**
- **Elasticsearch:** suporta BM25 nativo, mas é muito mais pesado (JVM, 1GB+ RAM)
- **SQLite FTS5:** mais leve, mas não distribuído e sem operações de conjunto eficientes
- **Arquivo invertido em disco:** sem latência de rede, mas sem distribuição e operações AND/OR em O(N log N)

### Chunking vs. documento inteiro

**Com chunking:**
- Posting lists menores (menor set intersection time)
- Resultados mais precisos (snippet relevante, não o documento inteiro)
- BM25 funciona melhor com documentos de comprimento similar

**Sem chunking:**
- Índice menor (menos chaves)
- TF mais preciso (conta o documento todo)
- Snippets exigem extração adicional

### Stemming vs. indexação de palavras completas

**Com stemming:**
- "apostadores", "apostou", "apostar" → mesmo stem `apost` → 1 posting list
- Maior revocação: busca por "apostar" encontra "apostadores"
- Índice menor (menos termos únicos)

**Sem stemming:**
- Precisão léxica perfeita
- Queries exatas funcionam melhor
- Necessita de expansão de consulta ou normalização para variações morfológicas

### aiohttp vs. FastAPI para a interface web

**aiohttp:**
- Já era dependência do projeto
- Sem overhead de validação automática (Pydantic)
- ~3 MB de dependência adicional

**FastAPI:**
- Documentação automática (Swagger)
- Tipagem e validação automática
- ~15 MB de dependências extras (Pydantic, Starlette)
- Não necessário para 2 endpoints simples

---

## 8. Bibliotecas Externas

| Biblioteca | Versão | Uso |
|---|---|---|
| **anyio** | 4.13+ | Runtime assíncrono (crawler workers) |
| **httpx** | 0.28+ | Cliente HTTP assíncrono com HTTP/2 |
| **redis[hiredis]** | 5.0+ | Cliente Redis assíncrono; hiredis = parser C nativo |
| **aiohttp** | 3.9+ | Servidor web (monitor + interface de busca) |
| **beautifulsoup4** | 4.14+ | Parser HTML para limpeza de texto |
| **lxml** | 6.1+ | Backend C para BeautifulSoup (10× mais rápido que html.parser) |
| **nltk** | 3.9+ | Tokenização (Punkt), stopwords, stemmers RSLP e Porter |
| **langdetect** | 1.0.9 | Detecção de idioma (n-gramas de caracteres) |
| **rich** | 14.3+ | Output formatado no terminal (REPL, CLI, dashboard TUI) |
| **ijson** | 3.5+ | Parser JSON streaming (corpus de GBs sem carregar em memória) |
| **zstandard** | 0.25+ | Descompressão do corpus `.zst` streaming |
| **motor** | 3.7+ | Driver MongoDB assíncrono |
| **pymongo** | — | Driver MongoDB síncrono (leitura do corpus) |
| **psutil** | 7.2+ | Métricas de CPU e memória no dashboard |
| **pydantic-settings** | 2.13+ | Configuração via variáveis de ambiente |
| **structlog** | 25.5+ | Logging estruturado (formato logfmt) |

---

## 9. Métricas de Desempenho

### Indexação (chunk_size = 100)

```
Documentos processados : 435
Chunks indexados       : 11.123
Tokens totais          : 1.087.902
Tempo total            : 119.4 s  (~2 min)
Throughput             : 3.64 docs/s
                         93.16 chunks/s
                         9.111 tokens/s
Termos únicos          : 8.052
Média postings/termo   : 125.94 chunks
Tamanho do índice      : 65.9 MB no Redis
```

### Busca (BM25, Redis local)

```
Tempo médio de resposta (interface web) : < 50 ms
Round-trips Redis por consulta          : 3 pipelines
  ├── 1× SCARD por termo (DF)
  ├── 1× GET idx:total_chunks (N)
  └── 1× HGETALL por chunk candidato (metadados)
```

### Crawler

```
Concorrência máxima    : 100 workers simultâneos
Limite por domínio     : 8 requisições simultâneas
Timeout HTTP           : 5 s
Throughput observado   : ~5-15 páginas/s (dependente dos servidores)
```

---

## 10. Como Executar

### Pré-requisitos

```bash
# Dependências
uv sync

# Infraestrutura (Redis + MongoDB)
docker-compose up -d redis mongodb
```

### Fase 1 — Crawler

```bash
# Iniciar o crawler
uv run python -m src.main

# Dashboard de monitoramento (outra aba)
uv run python -m src.monitor.server
# → http://localhost:8080
```

### Fase 2 — Indexação

```bash
# A partir do arquivo de corpus (.json.zst)
uv run python -m src.indexer.main --chunk-size 100

# A partir do MongoDB
uv run python -m src.indexer.main --mongo mongodb://localhost:27017

# Com métricas salvas
uv run python -m src.indexer.main --chunk-size 100 --metrics-out relatorio/metrics.json

# Restaurar índice a partir do dump JSONL
uv run python -m src.indexer.restore_index --dump relatorio/index_dump.jsonl
```

### Fase 3 — Busca

```bash
# Interface web
uv run python -m src.indexer.server
# → http://localhost:8888

# REPL interativo
uv run python -m src.indexer.repl

# CLI — busca única
uv run search "basketball odds" -k 10
uv run search --mode or "futebol gol" -k 5
uv run search "tennis match set" --plain
```

### Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | URL do Redis |
| `MONGODB_URI` | `mongodb://localhost:27017` | URI do MongoDB |
| `MAX_PAGES` | `50000` | Limite de páginas do crawler |
| `CONCURRENCY_LIMIT` | `100` | Workers simultâneos |
| `MAX_CONCURRENCY_PER_DOMAIN` | `8` | Limite por domínio |
| `TIMEOUT` | `5.0` | Timeout HTTP em segundos |

---

*Relatório gerado em Junho de 2026. Branch: `indexer`.*
