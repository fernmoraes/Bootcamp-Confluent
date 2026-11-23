# Plano — Pipeline de pagamentos end-to-end (Confluent Cloud)

> ## LEIA PRIMEIRO (para quem retoma este trabalho em um chat novo)
> Este arquivo é o diário de execução. Idioma: **português do Brasil**. Fonte da verdade da entrega: `Pipeline de pagamentos.pdf`. O README.md é o entregável final e é atualizado a cada sub-tarefa.
>
> **Regras de trabalho acordadas com o usuário:**
> 1. A cada sub-tarefa concluída: atualizar o `Status`, acrescentar "Notas para as próximas sub-tarefas" aqui, e preencher o README com a evidência real (saída de comando, nunca inventada).
> 2. Nunca imprimir segredos (`.env`, `.env.local`, `.state`). Antes de salvar evidência, conferir que nenhum segredo vazou (`grep -F` dos valores nos arquivos versionáveis).
> 3. Verificar comandos na documentação/`--help` antes de usar. Não afirmar nada que não foi observado.
> 4. Scripts novos de cada camada seguem `scripts/NN-nome.sh` (executável, idempotente, `source scripts/lib.sh`); o `setup.sh` os executa em ordem sozinho.
> 5. Terminal do usuário é zsh, mas os scripts são bash. Rodar verificações com `bash -c` (zsh não aceita `${!var}` e não separa `$c` sem aspas). macOS: não há `timeout`.
>
> **Como descobrir o estado real agora:** rode `./scripts/status.sh` (somente leitura). Para parar de gastar: `./teardown.sh --dry-run` e depois `./teardown.sh`. Para retomar: `./setup.sh`.
>
> **Painel de estado** (atualizar sempre):
>
> | Sub-tarefa | Status |
> |---|---|
> | 0 Scaffold | feito |
> | 1 Banco Neon + seed | feito |
> | 2 Fundação Confluent | feito |
> | 3 Contrato de dados (Schema Registry) | feito |
> | 4 Ingestão CDC | feito |
> | 5 Flink SQL | feito (refeito em 2026-11-23, pipeline real, 7/7 alertas) |
> | 6 Consumo | feito |
> | 7 Operação (métricas, acesso, custo, confiabilidade) | feito |
> | 8 setup/teardown (automação completa) | feito |
> | 9 README final com evidências reais | feito |
| 10 Encerramento (teardown + custo final) | feito em 2026-11-23: teardown executado, nada na Confluent; custo final US$ 189.33 bruto / US$ 0.00 líquido |
>
> **Estado atual (2026-11-23): TUDO DERRUBADO na Confluent Cloud (`./scripts/status.sh` → "environment desafio-final : NÃO EXISTE"). Neon mantido (2003 transações, sem slot). Para retomar: `./setup.sh`.**
>
> **Ambiente real em uso (IDs; segredos NÃO ficam aqui):** Neon projeto `late-lake-61849025` (us-east-1, org `org-round-wildflower-93544253`); Confluent org `ibm`, crédito `FREETRIAL400`. Os IDs da Confluent (`env-…`, `lkc-…`, `lsrc-…`, `sa-…`) mudam a cada teardown + setup: use `.state` ou `./scripts/status.sh`, não os valores escritos nas notas abaixo (são do primeiro ciclo).

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
- **Status:** `[x] done`
- **Notas para as próximas sub-tarefas:**
  - **Recursos criados (custo ativo!):** environment `desafio-final` (`env-6nj08j`), cluster Basic `desafio-basic` (`lkc-yow5gwo`, aws/us-east-1, single-zone, UP), Schema Registry `lsrc-w7w0zw5` (nasce sozinho com o 1º cluster; `schema-registry cluster enable` não existe), SAs `desafio-producer` (`sa-7p15z9p`) e `desafio-consumer` (`sa-o27kynx`). O environment `default` (`env-2kpg72`) já existia e não foi tocado. Conta tem crédito `FREETRIAL400` ($400, expira 16/12/2026) e cartão cadastrado.
  - **Arquivos:** `scripts/lib.sh` (helpers `load_env`, `state_set/state_load`, `retry`, `ensure_login`), `scripts/01-foundation.sh` (idempotente; rodado 2 vezes, a 2ª reaproveitou tudo), `scripts/evidence-foundation.sh`, `evidencias/02-fundacao.txt`. O `setup.sh` da Sub-tarefa 8 deve chamar esses scripts em ordem.
  - **`.state`** (gitignored, chmod 600) guarda: `ENV_ID`, `CLUSTER_ID`, `BOOTSTRAP`, `KAFKA_REST`, `SR_ID`, `SR_URL`, `PRODUCER_SA`, `CONSUMER_SA` e as 4 API keys (`PRODUCER_KAFKA_KEY/SECRET`, `CONSUMER_KAFKA_KEY/SECRET`, `PRODUCER_SR_KEY/SECRET`, `CONSUMER_SR_KEY/SECRET`). Os secrets só aparecem na criação, então NÃO apagar o `.state`; se apagar, é preciso recriar as keys.
  - **Mudança de convenção:** `TOPIC_PREFIX=desafio` (sem ponto) em `.env` e `.env.example`, para casar os tópicos `desafio.*` (ex.: `desafio.fraud.detected`). O prefixo de grupos continua `desafio-`.
  - **ACLs reais (Kafka):** producer = CREATE/WRITE/DESCRIBE em `desafio` (PREFIXED) + DESCRIBE/DESCRIBE_CONFIGS no cluster (exigido pelo Debezium V2, conforme a doc de ACLs de conectores). Consumer = READ/DESCRIBE em `desafio` (PREFIXED) + READ no grupo `desafio-` (PREFIXED). **Lacuna conhecida:** nenhum dos dois tem DESCRIBE_CONFIGS de tópico nem ACL de grupo para o producer; se o conector Debezium ou o Flink pedirem mais ACLs, adicionar em `01-foundation.sh` (Sub-tarefas 4 e 5). A doc diz que o Debezium V2 Postgres não precisa de ACL de `dbhistory` (só MySQL/SQL Server/MariaDB).
  - **Schema Registry:** usa RBAC, não ACL. Role bindings `DeveloperWrite` e `DeveloperRead` (producer) e `DeveloperRead` (consumer) em `Subject:desafio` (PREFIXED). `confluent iam rbac role-binding list` precisa de `--inclusive` para listar.
  - **Subjects:** o prefixo cobre `desafio.*`; subjects do conector têm a forma `<topic.prefix>.public.<tabela>-value`, então o `topic.prefix` do conector DEVE começar com `desafio`.
  - **Compatibilidade global do SR já é `BACKWARD`** (default), então a Sub-tarefa 3 só precisa confirmar e mostrar.
  - **Conector e Flink usam identidades próprias?** O plano usa a SA `desafio-producer` para o conector (precisa da key de Kafka dela, já no `.state`). Flink precisará de uma SA/permissão própria (statements rodam sob uma SA); decidir na Sub-tarefa 5 se reaproveita o producer (precisa de role `FlinkDeveloper`/`Assigner`) ou cria `desafio-flink`. NÃO verificado.
  - **Armadilhas do macOS:** não existe `timeout`/`gtimeout`; `confluent kafka topic consume` não tem timeout nativo (usar `&` + `kill`). `zsh` não aceita `${!var}`: rodar verificações com `bash -c`.
  - **Custo:** o cluster Basic cobra por hora desde a criação. Preço NÃO verificado; coletar com `confluent billing cost list` (há defasagem de dados). Rodar `teardown.sh` ao fim de cada sessão de trabalho.

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
- **Status:** `[x] done`
- **Notas para as próximas sub-tarefas:**
  - **Schemas registrados:** `schemas/customers.avsc` (ID 100001), `schemas/accounts.avsc` (ID 100002), `schemas/cards.avsc` (ID 100003), `schemas/merchants.avsc` (ID 100004), `schemas/transactions.avsc` (ID 100005), `schemas/fraud-detected.avsc` (ID 100006). Todos sob o namespace `com.bootcamp.payments.*`.
  - **Valores monetários:** `balance`, `amount`, `total_amount` tipados com precisão como `bytes` + `logicalType: decimal(15,2)`.
  - **Tags PII e PCI:** criadas no catálogo REST do Stream Governance (`/catalog/v1/types/tagdefs`) via permissão `DataSteward` (atribuída ao `PRODUCER_SA` em `03-schemas.sh`). Tags vinculadas aos campos sensíveis (`customers.full_name`, `email`, `phone`, `birth_date` com `PII`; `cards.card_number_mask` com `PCI`).
  - **Evolução e compatibilidade:** demonstrados com `confluent schema-registry schema compatibility validate`:
    - Adicionar campo opcional com default (`channel="POS"`, `device_id=null`): `Compatible | true` (aceito em `BACKWARD`).
    - Adicionar campo obrigatório sem default (`channel` sem default): `Compatible | false` (rejeitado em `BACKWARD`).
    - Remover campo obrigatório (`amount`): `Compatible | false` (rejeitado em `FULL`).
  - **Arquivos criados:** `schemas/*.avsc`, `schemas/test-evolution/*.avsc`, `scripts/03-schemas.sh` (executável e idempotente), `scripts/test-compat.sh`, `scripts/evidence-schemas.sh`, `evidencias/03-contrato-de-dados.txt`.
  - **Decisão CDC vs Domínio:** O conector Debezium (Sub-tarefa 4) gera envelopes Avro automaticamente no Schema Registry em `<topic.prefix>.public.<tabela>-value` com os campos `before`, `after`, `op`, `ts_ms`. O Flink SQL consome os envelopes CDC e projeta para o modelo analítico e de alerta (`desafio.fraud.detected`), cujo schema segue o contrato de dados registrado.

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
- **Status:** `[x] done`
- **Notas para as próximas sub-tarefas:**
  - **Conector CDC:** `desafio-cdc` (`lcc-9kyqqw7`), status `RUNNING`, task 0 `RUNNING`.
  - **Tópicos criados automaticamente:** `desafio.public.customers`, `desafio.public.accounts`, `desafio.public.cards`, `desafio.public.merchants`, `desafio.public.transactions` (1 partição, RF 3).
  - **Envelopes capturados:** `evidencias/01-envelopes-cdc.json` contém `insert_op_c` (`before=null`, `after`), `update_op_u` (`before` preenchido via `REPLICA IDENTITY FULL`, `after`), `delete_op_d` (`before` presente, `after=null`).
  - **Configurações:** `heartbeat.interval.ms=60000`, `decimal.handling.mode=precise`, `after.state.only=false`, `tombstones.on.delete=true`, slot `desafio_cdc_slot`.
  - **Arquivos:** `connectors/cdc.json`, `scripts/04-connector.sh` (executável e idempotente), `scripts/evidence-cdc.sh`, `evidencias/01-envelopes-cdc.json`.
  - **Atenção para Flink SQL (Sub-tarefa 5):** Os tópicos CDC contêm mensagens no formato de envelope Debezium Avro (`before`, `after`, `op`, `ts_ms`, etc.). No Confluent Cloud Flink SQL, os tópicos de Kafka aparecem como tabelas sob o catálogo/database padrão com colunas do schema Avro. Para consultar campos internos da tabela (`after`), avaliar se o Flink enxerga `after.<campo>` ou descompacta os campos diretamente.

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
- **Status:** `[x] done` (reaberta e refeita em 2026-11-23, ver "Correção")
- **Notas para as próximas sub-tarefas:**
  - **Compute pool Flink:** `desafio-pool` (`lfcp-w7wpx0g`, 5 max CFU, aws/us-east-1).
  - **Statements em execução (3):** `desafio-accounts-populate`, `desafio-enrichment`, `desafio-fraud-detection`, todos RUNNING e com `--property sql.tables.scan.idle-timeout=5s`. Os 3 sobem a partir de `scripts/05-flink.sh` (idempotente) e dos arquivos de `sql/`.
  - **CORREÇÃO (2026-11-23):** a 1ª versão desta sub-tarefa NÃO era reproduzível. O ambiente tinha statement `desafio-transactions-stream`, tópicos `raw/append/stream` e um `insert-burst` que injetava 3 transações (IDs 90001-90003) direto no Flink; o alerta "R$ 850.50" da evidência veio desses dados manuais, não do Postgres. Tudo isso foi removido (statements, tópicos e subjects do Schema Registry) e o pipeline foi refeito pelo caminho real.
  - **Ajustes de tabela feitos por DDL no `05-flink.sh`:** `desafio.public.transactions` -> `changelog.mode='append'`, coluna `txn_time` (de `txn_ts` STRING) como tempo de evento + watermark de 5 s; `desafio.accounts.versioned` com PK e `updated_at` como tempo de evento; watermark em `desafio.transactions.enriched.txn_ts`. Sem isso o join comum falhava com "sink doesn't support consuming update and delete changes".
  - **Regra de fraude (decisão validada):** o `MATCH_RECOGNIZE` roda ANTES do join, direto em `desafio.public.transactions` (`ORDER BY txn_time`), e só depois faz `JOIN ... FOR SYSTEM_TIME AS OF m.window_end` para pegar `customer_id`. Aplicar o padrão sobre `desafio.transactions.enriched` perdia 3 dos 7 alertas (cartões 114, 228, 320): `CURRENT_WATERMARK` mostrou as transações como atrasadas. Resultado final: 7 de 7 alertas (cartões 100, 114, 228, 245, 260, 320 do seed + 50 da rajada de teste inserida no Postgres) e nenhum alerta para 126 e 86.
  - **Armadilha de execução:** `DESCRIBE`/`SELECT` no Flink só devolvem linhas pela API REST (`/sql/v1/.../statements/<nome>/results`); a CLI não imprime resultado. Foi usada uma Flink API key TEMPORÁRIA (apagada ao final). Statements `SELECT` ad hoc só avançam watermark com `sql.tables.scan.idle-timeout=5s`.
  - **Atenção `enriched`:** o tópico `desafio.transactions.enriched` é alimentado (temporal join) mas hoje NINGUÉM o lê; é saída analítica. O watermark dele atrasa em relação à fonte (1 partição vs 6 de `accounts.versioned`).
  - **Arquivos:** `sql/01-enrichment.sql`, `sql/02-fraud-detection.sql`, `scripts/05-flink.sh`, `scripts/evidence-flink.sh` (agora SÓ com consultas reais: Kafka + Postgres), `evidencias/04-alerta-fraude.txt`.

### 6. Consumo
- **Intent:** mostrar quem lê o resultado usando a identidade `desafio-consumer`.
- **Expected Outcomes:** script de consumo com a API key do consumer lendo o tópico de alertas. Prova de que o consumer não consegue escrever (ACL negada).
- **Todo List:**
  1. Script `scripts/consume-alerts.sh` usando as credenciais do consumer.
  2. Teste negativo: tentativa de produzir com a identidade do consumer, com a saída de erro capturada.
- **Relevant Context:** PDF, Camadas 1 e 5 (Segurança).
- **Status:** `[x] done` (evidência regerada em 2026-11-23 com alerta real do pipeline)
- **Notas:**
  - Criados `scripts/06-consume.sh` e `scripts/consume-alerts.sh` usando a service account `desafio-consumer`.
  - Executado teste negativo de escrita: tentativa de produzir com a identidade do consumidor é rejeitada com `failed to produce offset -1: Broker: Topic authorization failed`, comprovando ACLs de menor privilégio.
  - Evidência real salva em `evidencias/05-consumo.txt` sem segredos.

### 7. Operação (Camada 5): quatro pilares
- **Intent:** responder às quatro perguntas operacionais com medições reais.
- **Expected Outcomes:** tabela no README com métricas via API, matriz de segurança, billing real e confiabilidade.
- **Status:** `[x] done`
- **Notas:**
  - Criados `scripts/metrics.sh`, `scripts/cost.sh` e `scripts/07-operation.sh`.
  - Observabilidade: Confluent Telemetry Metrics API consultada com sucesso (janela 2026-11-16 a 11-23 via `METRICS_START`: pico `received_bytes` 1.084.678 B; `sent_bytes` 7.019.047 B; `retained_bytes` 1.666.322 B). Lag por grupo NÃO medido: grupos efêmeros, a CLI devolve `None found`.
  - Segurança: Matriz completa de SAs, ACLs Kafka, RBAC de Schema Registry/DataSteward e permissões mínimas no Postgres.
  - Custos: Faturamento medido via `confluent billing cost list`: consumo BRUTO FINAL de US$ 189.33 (consulta DEPOIS do teardown, 2026-11-01 a 11-24; Flink 93.53, conector 72.50, cluster 23.22, rede ~0.085), coberto pelo crédito `FREETRIAL400` (-189.33; líquido US$ 0.00). (Valor anterior, com o ambiente ligado: 188.07.) Alavancas: (1) teardown entre sessões; (2) Flink no menor pool e sem statements sem consumidor.
  - Confiabilidade: Matriz trecho a trecho detalhando tolerâncias a falhas e o elo fraco na aplicação consumidora (mitigação via idempotência).
  - Evidência real salva em `evidencias/06-operacao.txt`.

### 8. `setup.sh` / `teardown.sh` idempotentes
- **Intent:** permitir subir do zero e derrubar tudo, evitando custo residual.
- **Expected Outcomes:** `./setup.sh` executa todas as camadas em ordem a partir do `.env`. `./teardown.sh` remove conector, statements e compute pool Flink, cluster, API keys, service accounts e environment.
- **Status:** `[x] done`
- **Notas:**
  - Pipeline 100% orquestrado em ordem estrita de dependências: `01-foundation.sh` $\rightarrow$ `03-schemas.sh` $\rightarrow$ `04-connector.sh` $\rightarrow$ `05-flink.sh` $\rightarrow$ `06-consume.sh` $\rightarrow$ `07-operation.sh`.
  - `teardown.sh` cobre destruição reversa completa incluindo conector, Flink compute pool/statements, role bindings (Developer e DataSteward), API keys, cluster, SAs, environment e slot de replicação Neon.

### 9. README final com evidências
- **Intent:** transformar as saídas reais em uma entrega nota alta.
- **Expected Outcomes:** README completo seguindo a estrutura do PDF com todas as evidências reais, justificativas técnicas e instruções de subida/derrubada.
- **Status:** `[x] done`
- **Notas:**
  - [`README.md`](README.md) 100% preenchido com evidências reais capturadas dos comandos, sem segredos vazados, justificativas de arquitetura, custos e matriz de confiabilidade.

## Ordem sugerida

0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9. Cada passo depende da saída do anterior. O teardown (8) deve ser executado no fim de cada sessão de teste para limitar o custo.

### 10. Encerramento (2026-11-23)
- **Status:** `[x] done`
- **Notas:**
  - **Estado encontrado:** conector `desafio-cdc` (`lcc-q2wnnv6`) `FAILED` (task 0 `USER_ACTIONABLE_ERROR`: posição de log armazenada indisponível no servidor, slot recriado/WAL descartado) e nenhum slot `desafio%` no Neon. Não foi investigada a causa do sumiço do slot.
  - **Decisão (vídeo vs. teardown):** o PDF só cita o "vídeo-gabarito" como referência do template; não há vídeo entre os itens do entregável. Gravar com o conector em `FAILED` mostraria um pipeline quebrado e custaria ~US$ 30/dia. Escolhido: teardown agora; as evidências já estão em `evidencias/` e no README. Para gravar vídeo no futuro: `./setup.sh` (recria tudo do zero, conector refaz o snapshot) e depois `./teardown.sh`.
  - **Executado:** `./teardown.sh --dry-run` e `./teardown.sh` (exit 0): conector, 8 statements, pool, role bindings, 4 API keys, cluster, 2 SAs e environment apagados; `None found.` nas listas; slots Neon restantes: 0. Saída real colada no README ("Verificação de Listas Vazias pós-Teardown").
  - **Custo final:** US$ 189.33 bruto, US$ 0.00 líquido (README e nota da sub-tarefa 7 atualizados). A última linha diária pode sofrer ajuste por defasagem do billing.
  - **Pendência:** o `.state` foi removido pelo teardown; o README "Como subir" continua válido.
