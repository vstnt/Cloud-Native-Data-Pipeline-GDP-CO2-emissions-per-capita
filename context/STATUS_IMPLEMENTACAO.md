# Status da Implementação

Checklist alinhado aos "Grandes Passos da Implementação" (`context/Passos da implementação.pdf`).

## 1. Parte 1 – Implementação LOCAL

- [x] (1) Criar a estrutura de pastas do projeto
- [x] (2) Implementar módulo de METADATA (DynamoDB abstraction, local/mocked)
- [x] (3) Implementar INGESTÃO da World Bank API (RAW)
- [x] (4) Implementar PROCESSAMENTO da World Bank API (PROCESSED)
- [x] (5) Implementar CRAWLER da Wikipedia (RAW)
- [x] (6) Implementar PROCESSAMENTO da tabela de CO₂
- [x] (7) Implementar a CURATED LAYER
- [x] (8) Implementar o Analytical Output
- [x] (9) Implementar a ORQUESTRAÇÃO LOCAL (entrypoint local)

## 2. Parte 2 – Preparar para levar à AWS (infra + adaptação do código)

- [x] (10) Criar bucket S3 e projetar a estrutura de prefixos
- [x] (11) Criar tabela DynamoDB real
- [x] (12) Ajustar caminho dos arquivos para S3
- [ ] (13) Criar função AWS Lambda
- [ ] (14) Criar IAM Role do Lambda
- [ ] (15) Criar regra EventBridge (agendamento diário)
- [ ] (16) Testar execução na AWS
- [ ] (17) Preencher os documentos finais

