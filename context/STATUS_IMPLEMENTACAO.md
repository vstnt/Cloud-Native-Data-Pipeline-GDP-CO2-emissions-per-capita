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

Notas da Parte 2 (atualização):

- Infraestrutura como código adicionada em `cloud/lambda/`:
  - `Dockerfile` (Lambda container image com dependências).
  - `template.yaml` (CloudFormation: Lambda + Role + EventBridge Rule).
  - `build_and_deploy.sh` (script para build→push ECR→deploy do stack).
- Itens (13), (14) e (15) ficam prontos para deploy; dependem apenas de executar o script no WSL com AWS CLI + Docker.
- Analytical Output agora também persiste em S3 (prefixo `analytics/<YYYYMMDD>/`) quando rodando via Lambda (StorageAdapter). Em execução local, continua a escrever em `analysis/`.
