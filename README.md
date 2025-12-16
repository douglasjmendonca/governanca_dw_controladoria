# Plataforma de Dados — Governança & Rastreabilidade (Starter)

Este repositório integra **documentação como código** (PlantUML + Markdown) e **infraestrutura como código** (Terraform),
seguindo boas práticas de governança, rastreabilidade e versionamento.

## Pastas
- `docs/` — Documentação técnica (diagramas PlantUML, dicionário de dados, ADRs, pipelines).
- `infra/` — Terraform (módulos + ambientes). Usa *remote state* e *workspaces*.
- `.github/workflows/` — CI/CD para render de diagramas, validação Terraform e publicação de docs.

## Como usar
1. **Diagramas**: edite arquivos `.puml` em `docs/diagrams`. O workflow gera `.svg` automaticamente.
2. **Pipelines**: descreva em `docs/pipelines/*.md` usando o *template*.
3. **Dicionário de dados**: crie arquivos por tabela em `docs/dicionario_dados/*.md`.
4. **Infraestrutura**: defina recursos nos módulos Terraform em `infra/modules/*` e componha em `infra/envs/*`.
5. **Governança**: registre decisões em `docs/adr/*.md`, siga *Conventional Commits* e *CODEOWNERS*.

## Padrões
- **Conventional Commits**: `feat:`, `fix:`, `docs:`, `infra:`, `chore:`…
- **Semantic Versioning**: versões em `CHANGELOG.md`.
- **Data Contracts**: JSON Schema em `docs/pipelines/contracts/` (se aplicável).
