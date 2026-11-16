# Plano — Pipeline de pagamentos end-to-end (Confluent Cloud)

## Visão geral

**Objetivo:** entregar um repositório público no GitHub com um pipeline de pagamentos reproduzível:
Neon (Postgres) → CDC Debezium (`PostgresCdcSourceV2`) → tópicos Avro → Flink SQL (enriquecimento + regra de fraude) → consumo.
O README deve ter, por camada: o que faz, evidência de execução, por que a peça foi escolhida, como subir/derrubar, e o custo.

**Fonte de verdade da estrutura:** `Pipeline de pagamentos.pdf` (esqueleto nota-alta). Os trechos `<sua evidência aqui>` são preenchidos com a saída real da execução.

**Decisões já confirmadas com o usuário:**
- Seguir o PDF à risca (Neon, Debezium V2, Avro, Flink SQL, cluster Basic aws/us-east-1, `setup.sh`/`teardown.sh`, `.env`/`.env.example`).
- Os dados sintéticos **não existem** no workspace. É preciso criar um gerador determinístico (`seed=42`) com o schema deduzido do PDF.
- O usuário ainda não tem conta Confluent Cloud, conta Neon nem Confluent CLI. Isso entra como pré-requisito no README.

**Restrições e riscos:**
- As evidências só existem depois de executar contra Confluent Cloud e Neon reais, o que gera custo. A implementação prepara scripts e arquivos, e o usuário executa e captura as saídas.
- Nenhum secret vai para o repositório (`.env` no `.gitignore`, conectores com variáveis de ambiente).
- Sintaxe exata da CLI, do connector config e do Flink SQL deve ser verificada na documentação oficial durante a implementação. Não assumir de memória.
- Ainda não é seguro afirmar números de custo nem valores de métricas. Eles vêm só da execução real.

## Estrutura de repositório proposta

```
.env.example
.gitignore
README.md
setup.sh
teardown.sh
db/        # DDL + gerador determinístico seed=42 + criação do cdc_user
schemas/   # Avro (com.bootcamp.payments.*)
connectors/cdc.json
sql/       # statements Flink SQL
scripts/   # helpers: ACLs, test-compatibility, consumo, métricas, billing
evidencias/ # saídas capturadas (envelopes CDC, alertas, listas vazias)
```

## Sub-tarefas

### 0. Scaffold do repositório e pré-requisitos
- **Intent:** criar a base do repo e documentar como obter contas e ferramentas.
- **Expected Outcomes:** repo com estrutura de pastas, `.gitignore` cobrindo `.env`, `.env.example` com todas as variáveis, seção de pré-requisitos no README.
- **Todo List:**
  1. Criar a estrutura de diretórios acima.
  2. Escrever `.gitignore` (`.env`, arquivos de credenciais, `.bob/`).
  3. Escrever `.env.example` (credenciais Neon, nomes de ambiente/cluster, região, prefixo `desafio-`, sem valores reais).
  4. Redigir o rascunho do README: pré-requisitos (criar conta Confluent Cloud, conta Neon, instalar Confluent CLI, `psql`, Python).
  5. Pesquisar na documentação a versão e os comandos atuais da CLI antes de fixá-los nos scripts.
- **Relevant Context:** `Pipeline de pagamentos.pdf` (cabeçalho: reproduzível com `./setup.sh`, pré-requisito `.env`).
- **Status:** `[x] done`
- **Notas para as próximas sub-tarefas:**
  - **PENDÊNCIA DO USUÁRIO:** a ferramenta de escrita bloqueou a criação de `.gitignore` (padrão de ignore). O conteúdo está em `gitignore.template`. Rodar `mv gitignore.template .gitignore` antes de qualquer `git init`/commit. Não há `.git` no workspace ainda (o diretório pai também não é repo).
  - Criados: `db/ schemas/ connectors/ sql/ scripts/ evidencias/` (cada um com `.gitkeep`), `.env.example`, `README.md` (rascunho com placeholders), `gitignore.template`. Remover os `.gitkeep` é opcional quando a pasta tiver conteúdo.
  - `.env.example` define: `CONFLUENT_CLOUD_EMAIL/PASSWORD`, `ENV_NAME`, `CLUSTER_NAME`, `CLUSTER_CLOUD`, `CLUSTER_REGION`, `PRODUCER_SA_NAME`, `CONSUMER_SA_NAME`, `CONSUMER_GROUP_PREFIX`, `TOPIC_PREFIX`, `FLINK_COMPUTE_POOL_NAME`, `CONNECTOR_NAME`, `PG_HOST/PORT/DATABASE`, `PG_ADMIN_USER/PASSWORD`, `CDC_USER/CDC_PASSWORD`, `PG_SSLMODE`, `SEED`. Os nomes de `FLINK_COMPUTE_POOL_NAME` e `CONNECTOR_NAME` (`desafio-pool`, `desafio-cdc`) são escolha minha, não vêm do PDF. Acrescentar variáveis novas (IDs, API keys) no arquivo de estado local, não no `.env.example`.
  - O `.gitignore` já cobre `.state`, `.state/`, `*.state`, `state.json`, `credentials.json`; o `setup.sh` (Sub-tarefa 2) deve gravar o estado em um desses nomes (sugestão: `.state`).
  - **Máquina de desenvolvimento:** Confluent CLI `v4.70.0` instalada (`/opt/homebrew/bin/confluent`); `python3` 3.14.5; `psql` NÃO está instalado (libpq existe no Homebrew, é keg-only e exige ajuste de PATH, ver README); Docker não foi verificado. Login na Confluent Cloud não foi feito e nenhum recurso foi criado.
  - **Comandos confirmados via `--help` na v4.70.0** (existem; semântica completa ainda a validar ao usar): `login` (aceita `CONFLUENT_CLOUD_EMAIL`/`CONFLUENT_CLOUD_PASSWORD`), `environment create <name>` (flag `--governance-package essentials|advanced`, default `essentials`), `kafka cluster create <name>` (`--cloud`, `--region` obrigatórios; `--type` default `basic`; `--availability`), `iam service-account create|list`, `kafka acl create|list`, `api-key create --resource <lkc|lsrc|flink|cloud|global> [--service-account]`, `connect cluster create|list`, `flink compute-pool create <name>`, `flink statement create|list` (`--sql`, `--compute-pool`, `--service-account`, `--database`, `--property k=v`, `--wait`), `billing cost list --start-date --end-date`, `kafka topic produce|consume`, `schema-registry schema create`, `schema-registry schema compatibility validate <arquivo> --type avro --subject S --version latest`, `schema-registry cluster describe|update`.
  - **Atenção, difere do que o plano assumia:** `schema-registry cluster enable` NÃO existe na v4.70.0 (só `describe` e `update`). A habilitação do Schema Registry precisa ser reavaliada na Sub-tarefa 2 (pode vir junto com o environment e o Stream Governance package; confirmar na doc). Também não verifiquei ainda `iam service-account`/ACL semantics em cluster Basic, nem `confluent flink region`.
  - O comando de teste de compatibilidade correto é `confluent schema-registry schema compatibility validate` (usar na Sub-tarefa 3, não `test-compatibility`).
  - Neon: confirmado na doc que `psql` conecta por connection string via SSL na porta 5432 (`sslmode=require`). Habilitar logical replication na Neon NÃO foi verificado; fica para a Sub-tarefa 1 (item 6).
  - Fontes consultadas: <https://docs.confluent.io/confluent-cli/current/install.html>, <https://docs.confluent.io/confluent-cli/current/command-reference/confluent_login.html>, <https://neon.com/docs/connect/query-with-psql-editor>, mais `confluent <cmd> --help` local. As bibliotecas IBM e o MCP do watsonx Orchestrate não cobrem Confluent/Neon.

### 1. Dados sintéticos e banco (Neon)
- **Intent:** ter o Postgres de origem com dados determinísticos e configurado para CDC.
- **Expected Outcomes:** tabelas `customers`, `accounts`, `cards`, `merchants`, `transactions` com contagens 200 / 240 / 320 / 60 / 2000. Dois execuções do gerador produzem o mesmo resultado.
- **Todo List:**
  1. Desenhar o DDL deduzindo do PDF: PK em todas as tabelas, relações customer→account→card, transação ligada a `card_id`, `account_id` e merchant, valor monetário `numeric(15,2)`, timestamp.
  2. Incluir `REPLICA IDENTITY FULL` nas tabelas capturadas, para o `before` do UPDATE vir preenchido.
  3. Criar a publication e o usuário `cdc_user` com privilégios mínimos (replicação + SELECT nas tabelas).
  4. Escrever o gerador determinístico (`seed=42`, p.ex. Python + Faker ou `random.Random(42)`). Deve incluir cartões que gerem rajadas de 3+ transações em menos de 60s, para a regra de fraude disparar de forma controlada.
  5. Script de verificação com `SELECT count(*)` por tabela (evidência do quadro de contagens).
  6. Confirmar nos docs da Neon a habilitação de logical replication.
- **Relevant Context:** PDF, Camada 3 (tabela de contagens, `REPLICA IDENTITY FULL`, `cdc_user`). Pasta `db/`.
- **Status:** `[x] done`
- **Notas para as próximas sub-tarefas:**
  - **Projeto Neon:** `bootcamp-confluent-use1` (ID `late-lake-61849025`), `aws-us-east-1`, branch `main`, database `neondb`. Logical replication HABILITADA (irreversível) via `neon projects update <id> --enable-logical-replication --yes`; `wal_level=logical` confirmado.
  - **`.env`** (gitignored, `chmod 600`) preenchido a partir de `DATABASE_URL_UNPOOLED` (host direto, sem `-pooler`, obrigatório para CDC): `PG_HOST/PORT/DATABASE/ADMIN_USER/ADMIN_PASSWORD` + `CDC_PASSWORD` gerado aleatoriamente. Faltam `CONFLUENT_CLOUD_EMAIL/PASSWORD`.
  - **Arquivos:** `db/01-schema.sql`, `db/02-cdc-user.sql`, `db/03-verify.sql`, `db/seed.py` (só stdlib, determinístico: sha256 `5e292bef…b136`, idêntico em 2 execuções), `db/apply.sh` (idempotente: recria schema, cdc_user e seed), `evidencias/00-seed-e-banco.txt`.
  - **Esquema (para os Avro da Sub-tarefa 3):** `customers(customer_id PK, full_name, email, phone, birth_date, city, created_at)`; `accounts(account_id PK, customer_id, account_type, status, risk_tier, balance numeric(15,2), currency, opened_at, updated_at)`; `cards(card_id PK, account_id, card_number_mask, brand, status, expires_on, issued_at)`; `merchants(merchant_id PK, name, mcc, category, city, country)`; `transactions(transaction_id PK bigint, card_id, account_id, merchant_id, amount numeric(15,2), currency, status, txn_ts timestamptz)`. PII: customers.full_name/email/phone/birth_date. PCI: cards.card_number_mask.
  - **Para o conector (Sub-tarefa 4):** publication `desafio_pub` já existe (as 5 tabelas), `REPLICA IDENTITY FULL` em todas, e `cdc_user` tem LOGIN+REPLICATION+SELECT. **O slot de replicação NÃO foi criado**: deixar o conector criá-lo (confirmar `slot.name` e `publication.name`/`publication.autocreate.mode` no `connectors/cdc.json`). Teste feito: `cdc_user` lê (SELECT ok), é negado em escrita e consegue criar/derrubar slot `pgoutput`.
  - **Cenários de fraude do seed** (verdade esperada para a Sub-tarefa 5): ALERTA nos cartões 100, 245, 320, 114, 260, 228 (rajadas de 3, 3, 4, 3, 5, 3 transações em ≤ 40 s); SEM alerta no cartão 126 (2 txns em 30 s) e no 86 (3 txns espaçadas de 45 s, ou seja, 90 s). Os demais cartões nunca têm duas transações a menos de 300 s. `txn_ts` fica em outubro/2026 (janela fixa), então o Flink precisa usar `txn_ts` como event time, não o relógio de ingestão.
  - **Custo/Neon:** o conector mantém o compute ativo (sem scale-to-zero) enquanto estiver conectado, segundo a doc da Neon; registrar na seção de custos. Plano e preço da Neon NÃO foram verificados.
  - **Máquina:** `libpq` instalada via `brew` (psql 18.7 em `/opt/homebrew/opt/libpq/bin`, fora do PATH; `db/apply.sh` já resolve isso).
  - **IP Allow:** não verificado se está habilitado na Neon; se estiver, será preciso liberar os IPs do conector.

### 2. Fundação (Camada 1)
- **Intent:** criar environment, cluster e identidades com menor privilégio.
- **Expected Outcomes:** environment `desafio-final`, cluster Basic `desafio-basic` (aws/us-east-1), Schema Registry habilitado, `desafio-producer` (WRITE em `desafio-*`) e `desafio-consumer` (READ em `desafio-*` + grupo `desafio-*`). A conta pessoal só é usada em setup/teardown.
- **Todo List:**
  1. Em `setup.sh`: login, criação do environment e do cluster, com wait até o cluster ficar pronto.
  2. Criar as duas service accounts, API keys por SA e ACLs com prefixo.
  3. Habilitar o Schema Registry e criar a API key dele.
  4. Fazer o script salvar os IDs gerados num arquivo de estado local (não versionado).
  5. Script de evidência: `confluent iam service-account list` + `confluent kafka acl list` por SA.
  6. Verificar nos docs: ACLs em cluster Basic e a ordem criar SA → ACL → API key.
- **Relevant Context:** PDF, Camada 1; `setup.sh`, `scripts/`.
- **Status:** `[ ] pending`

### 3. Contrato de dados (Camada 2)
- **Intent:** registrar os schemas e provar a evolução compatível e a rejeição de quebra.
- **Expected Outcomes:** schemas Avro em `schemas/`, namespace `com.bootcamp.payments.*`, valores monetários como `bytes` + `decimal(15,2)`, tags PII/PCI nos campos sensíveis. Compatibilidade BACKWARD configurada. Duas evidências: `test-compatibility` aceita campo novo com default e REJEITA remoção de campo obrigatório.
- **Todo List:**
  1. Escrever os `.avsc` das entidades e do alerta de fraude (`desafio.fraud.detected`).
  2. Definir como registrar tags/metadados PII/PCI (checar nos docs de Stream Governance o suporte no tier disponível).
  3. Definir o nível de compatibilidade BACKWARD por subject.
  4. Script `scripts/test-compat.sh` com os dois cenários (versão candidata A: campo novo com default; versão B: remove campo obrigatório) e captura de saída.
  5. Decidir se os tópicos de CDC usam schema registrado manualmente ou gerado pelo conector, e documentar a escolha.
- **Relevant Context:** PDF, Camada 2; pasta `schemas/`.
- **Status:** `[ ] pending`

### 4. Ingestão CDC (Camada 3)
- **Intent:** ligar o Postgres ao Kafka e mostrar INSERT, UPDATE e DELETE como eventos.
- **Expected Outcomes:** conector `PostgresCdcSourceV2` rodando, `connectors/cdc.json` sem secrets, `heartbeat.interval.ms=60000`. Evidência: `evidencias/01-envelopes-cdc.json` com `op=c` (`before=null`), `op=u` (`before` e `after`), `op=d` (`after=null` + tombstone).
- **Todo List:**
  1. Escrever `connectors/cdc.json` com placeholders de env vars (substituição via `envsubst` ou similar no `setup.sh`).
  2. Criar o conector pela CLI e aguardar `RUNNING`.
  3. Script que executa um INSERT, um UPDATE e um DELETE controlados no Postgres.
  4. Consumir os tópicos com Avro e salvar os três envelopes em `evidencias/01-envelopes-cdc.json`.
  5. Documentar o motivo do `cdc_user` com menor privilégio e do heartbeat.
- **Relevant Context:** PDF, Camada 3; `connectors/cdc.json`, `db/`.
- **Status:** `[ ] pending`

### 5. Processamento Flink SQL (Camada 4)
- **Intent:** enriquecer transações com contas e emitir alertas de fraude.
- **Expected Outcomes:** compute pool Flink; statements em `sql/`: (a) temporal join transações ⋈ contas (PK nos dois lados, `sql.tables.scan.idle-timeout=5s`); (b) `RULE_VELOCITY` com `MATCH_RECOGNIZE` (`PARTITION BY card_id`, `PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND`) gravando em `desafio.fraud.detected`. Evidência: o alerta no tópico mais as 3 transações do mesmo `card_id` (<60s).
- **Todo List:**
  1. Criar o compute pool e fixar a região igual à do cluster.
  2. Escrever o SQL do enriquecimento (join com `accounts`), com atenção à time attribute e à semântica de PK/changelog.
  3. Escrever o SQL de fraude e a tabela de saída `desafio.fraud.detected`.
  4. Script de deploy dos statements (`confluent flink statement create`) com o idle-timeout.
  5. Injetar uma rajada controlada de 3 transações no mesmo cartão, consumir o alerta e correlacionar as 3 transações de origem.
  6. Verificar na documentação: suporte a `MATCH_RECOGNIZE` e a temporal join no Flink do Confluent Cloud, e o comportamento do `PATTERN` com `{3,}?` e `WITHIN`.
- **Relevant Context:** PDF, Camada 4; pasta `sql/`.
- **Status:** `[ ] pending`

### 6. Consumo
- **Intent:** mostrar quem lê o resultado usando a identidade `desafio-consumer`.
- **Expected Outcomes:** script de consumo com a API key do consumer lendo o tópico de alertas. Prova de que o consumer não consegue escrever (ACL negada).
- **Todo List:**
  1. Script `scripts/consume-alerts.sh` usando as credenciais do consumer.
  2. Teste negativo: tentativa de produzir com a identidade do consumer, com a saída de erro capturada.
- **Relevant Context:** PDF, Camadas 1 e 5 (Segurança).
- **Status:** `[ ] pending`

### 7. Operação (Camada 5): quatro pilares
- **Intent:** responder às quatro perguntas operacionais com medições reais.
- **Expected Outcomes:** tabela no README com:
  1. **Observabilidade:** pico de `received_bytes` e lag por grupo via Metrics API (`curl`).
  2. **Segurança:** tabela SA → ACLs → porquê.
  3. **Custos:** `confluent billing cost list` do período + 2 alavancas de redução.
  4. **Confiabilidade:** tabela trecho a trecho, com o elo fraco (consumer → efeito, idempotência fica no código do consumidor).
- **Todo List:**
  1. Script `scripts/metrics.sh` com a query na Metrics API (checar nos docs o endpoint, a autenticação e as métricas disponíveis).
  2. Script de billing (`scripts/cost.sh`); considerar a defasagem do billing e documentá-la.
  3. Redigir a tabela de segurança a partir das ACLs reais da sub-tarefa 2.
  4. Redigir a tabela de confiabilidade analisando cada trecho (Postgres → conector → Kafka → Flink → consumer).
- **Relevant Context:** PDF, Camada 5.
- **Status:** `[ ] pending`

### 8. `setup.sh` / `teardown.sh` idempotentes
- **Intent:** permitir subir do zero e derrubar tudo, evitando custo residual.
- **Expected Outcomes:** `./setup.sh` executa todas as camadas em ordem a partir do `.env`. `./teardown.sh` remove conector, statements e compute pool Flink, cluster, API keys, service accounts e environment. Evidência de listas vazias.
- **Todo List:**
  1. Consolidar os passos das sub-tarefas 2 a 5 no `setup.sh` (`set -euo pipefail`, validação do `.env`, esperas explícitas).
  2. Escrever o `teardown.sh` em ordem reversa de dependência.
  3. Passo final de verificação: `connect cluster list` / `flink statement list` retornando "None found".
  4. Orientar a limpeza do banco Neon (opcional).
- **Relevant Context:** PDF, "Custo total e teardown".
- **Status:** `[ ] pending`

### 9. README final com evidências
- **Intent:** transformar as saídas reais em uma entrega nota alta.
- **Expected Outcomes:** README completo seguindo a estrutura do PDF: visão geral, diagrama, uma seção por camada com evidência, justificativas das escolhas, passos de subida/derrubada, custo declarado (`billing cost list`).
- **Todo List:**
  1. Executar o pipeline real (usuário), salvando as saídas em `evidencias/`.
  2. Trocar cada `<sua evidência aqui>` pela saída real.
  3. Escrever as justificativas (por que Neon, Debezium V2, Avro, Flink SQL, Basic, etc.), usando só o que foi observado.
  4. Preencher o custo total e confirmar o teardown.
  5. Publicar no GitHub (público) após conferir que não há secrets no histórico.
- **Relevant Context:** PDF inteiro; `evidencias/`.
- **Status:** `[ ] pending`

## Ordem sugerida

0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9. Cada passo depende da saída do anterior. O teardown (8) deve ser executado no fim de cada sessão de teste para limitar o custo.
