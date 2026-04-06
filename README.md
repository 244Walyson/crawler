# Crawler de Odds Esportivas

Este projeto é um coletor (crawler) de alto desempenho desenvolvido para a disciplina de Recuperação de Informações na Web e Redes Sociais. O objetivo é coletar e comparar odds de eventos esportivos de diversas fontes.

## 1. Proposta do Sistema de RI

O sistema visa auxiliar apostadores e analistas a encontrar as melhores oportunidades de apostas em eventos de futebol, consolidando dados de múltiplos provedores em um único local.

## 2. Descrição do Coletor

- **Tipo**: Coletor focado (Focused Crawler) em sites de agregação de odds esportivas.
- **Propriedades**:
    - **Assíncrono**: Utiliza `httpx` e `anyio` para realizar múltiplas requisições simultâneas sem bloquear o processo.
    - **Alta Performance**: O parsing do HTML é feito com `selectolax` (baseado em Lexbor), que é significativamente mais rápido que BeautifulSoup.
    - **Concorrência**: Suporta múltiplos workers configuráveis via variáveis de ambiente.
- **Políticas**:
    - **Escopo**: Limitado aos domínios permitidos (ex: OddsPortal, BetExplorer).
    - **Polidez**: Delay configurável entre requisições para evitar sobrecarga nos servidores alvo e bloqueios.
    - **Deduplicação**: Mantém um registro de URLs visitadas e enfileiradas para evitar coletas redundantes.
- **Critério de Parada**: O crawler para automaticamente ao atingir o limite de páginas configurado (ex: 50.000 páginas para pontuação máxima).
- **Justificativa**: A escolha de Python com `uv` e bibliotecas assíncronas modernas garante um desenvolvimento rápido, código elegante e performance necessária para escalas de dezenas de milhares de páginas.

## 3. Escala

O sistema foi projetado para escalar horizontalmente. Para atingir a meta de 50.000 páginas, basta configurar o `MAX_PAGES` no arquivo `.env` e ajustar a concorrência conforme a capacidade da rede.

## Como Executar

1. Certifique-se de ter o `uv` instalado.
2. Clone o repositório.
3. Configure o arquivo `.env` (exemplo fornecido).
4. Execute o crawler:
   ```bash
   uv run python -m src.main
   ```

Os dados coletados serão salvos em `data.jsonl`.
