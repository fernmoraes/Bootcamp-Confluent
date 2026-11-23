# Desafio Final — Pipeline de Pagamentos End-to-End

Pipeline de streaming de pagamentos em tempo real: **Neon (Postgres)** $\rightarrow$ **CDC Debezium (`PostgresCdcSourceV2`)** $\rightarrow$ **Tópicos Avro + Schema Registry (Tags PII/PCI)** $\rightarrow$ **Flink SQL (Temporal Join + Detecção de Fraude CEP `MATCH_RECOGNIZE`)** $\rightarrow$ **Consumo Seguro (`desafio-consumer`)**.

Reproduzível com `./setup.sh` (pré-requisito: `.env` preenchido a partir do `.env.example`).

## Pré-requisitos

### 1. Conta Confluent Cloud
Crie a conta em <https://confluent.cloud/signup>. A página de instalação da CLI menciona US$ 400 em créditos gratuitos para novos usuários do Confluent Cloud; confira as condições vigentes no momento do cadastro.

### 2. Conta Neon (Postgres)
Crie a conta em <https://console.neon.tech>. Você vai precisar do host, banco, usuário e senha para preencher o `.env`. O CDC exige logical replication no projeto Neon: habilite com `neon projects update <id> --enable-logical-replication --yes` (**irreversível** para aquele projeto; `db/apply.sh` aborta se `wal_level` não for `logical`).

### 3. Confluent CLI
Documentação oficial: <https://docs.confluent.io/confluent-cli/current/install.html>

| Sistema | Comando (conforme a documentação) |
|---|---|
| macOS / Linux (Homebrew) | `brew install confluentinc/tap/cli` |
| Debian / Ubuntu (APT) | Adicionar a chave e o repositório APT e depois `sudo apt install confluent-cli` (passo a passo na página acima) |
| Windows | Baixar o ZIP mais recente em <https://github.com/confluentinc/cli/releases/latest>, descompactar e executar `confluent.exe` |
| Docker | `docker pull confluentinc/confluent-cli:latest` |

Verificar a instalação:

```bash
confluent version
```

Atualizar uma instalação existente:

```bash
confluent update
```

**Versão em que os comandos foram verificados:** `v4.70.0` (saída de `confluent version` na máquina de desenvolvimento). Isso significa que os nomes e flags dos comandos usados no projeto foram conferidos com `confluent <comando> --help` nessa versão. Versões diferentes podem divergir.

Login não interativo (usado pelo `setup.sh`): a documentação de `confluent login` indica que o login por e-mail e senha pode ser feito com as variáveis de ambiente `CONFLUENT_CLOUD_EMAIL` e `CONFLUENT_CLOUD_PASSWORD`. Contas com SSO não suportam login não interativo; nesse caso execute `confluent login` manualmente antes do `setup.sh`.

### 4. psql (cliente Postgres)
Conforme a documentação da Neon (<https://neon.com/docs/connect/query-with-psql-editor>):

| Sistema | Comando |
|---|---|
| macOS (Apple Silicon) | `brew install libpq` e depois `echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc && source ~/.zshrc` |
| macOS (Intel) | `brew install libpq` e depois `echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc && source ~/.zshrc` |
| Linux (Debian/Ubuntu) | `sudo apt update && sudo apt install postgresql-client` |
| Windows | Instalador em <https://www.postgresql.org/download/windows/> |

O pacote `libpq` do Homebrew é *keg-only*: não é linkado no PATH automaticamente, por isso o `export PATH` é necessário.

### 5. Python 3
Necessário para o gerador determinístico de dados (`seed=42`, `db/seed.py`) e para utilitários dos scripts. O gerador usa só a biblioteca padrão e foi testado com Python 3.14.5 (`python3 --version`). Não foi testado com outras versões.

### 6. Configurar o `.env`

```bash
cp .env.example .env
# edite o .env com suas credenciais; ele está no .gitignore e nunca deve ser versionado
```

## Estrutura do repositório

```
.env.example
.gitignore
README.md
pipeline-pagamentos-plan.md  # plano + diário de execução (ler primeiro ao retomar)
setup.sh          # sobe/retoma tudo (idempotente)
teardown.sh       # derruba tudo (--dry-run para simular)
db/               # DDL + gerador determinístico seed=42 + cdc_user + apply.sh
schemas/          # Avro (com.bootcamp.payments.*)
connectors/       # cdc.json
sql/              # statements Flink SQL
scripts/          # lib.sh, NN-*.sh (uma por camada), status.sh, evidence-*.sh
evidencias/       # saídas capturadas
```

## Camada 1 — Fundação

Environment `desafio-final` + cluster Basic `desafio-basic` (aws/us-east-1). O Schema Registry nasce junto com o primeiro cluster do environment (`confluent schema-registry cluster describe` falha com "Schema Registry not enabled" antes disso).

Identidades: `desafio-producer` (WRITE em `desafio*`), `desafio-consumer` (READ em `desafio*` + grupo `desafio-*`); conta pessoal só em setup/teardown. Script: `scripts/01-foundation.sh` (idempotente). Evidência completa: `evidencias/02-fundacao.txt`.

```
$ confluent iam service-account list
      ID     |       Name       |          Description
  sa-0jm6vv2 | desafio-producer | Escreve nos topicos desafio-*
  sa-mkxgqqq | desafio-consumer | Le os topicos desafio-*

$ confluent kafka acl list --service-account sa-0jm6vv2     # desafio-producer
  ALLOW | CREATE            | TOPIC   | desafio       | PREFIXED
  ALLOW | DESCRIBE          | TOPIC   | desafio       | PREFIXED
  ALLOW | WRITE             | TOPIC   | desafio       | PREFIXED
  ALLOW | DESCRIBE          | CLUSTER | kafka-cluster | LITERAL
  ALLOW | DESCRIBE_CONFIGS  | CLUSTER | kafka-cluster | LITERAL

$ confluent kafka acl list --service-account sa-mkxgqqq     # desafio-consumer
  ALLOW | READ     | TOPIC | desafio  | PREFIXED
  ALLOW | DESCRIBE | TOPIC | desafio  | PREFIXED
  ALLOW | READ     | GROUP | desafio- | PREFIXED
```

Prova de menor privilégio (comandos reais com a API key de cada identidade):

```
producer escreve em desafio.probe                       PERMITIDO
consumer tenta escrever em desafio.probe                NEGADO
producer tenta escrever fora do prefixo (outro.topico)  NEGADO
consumer lê desafio.probe                               PERMITIDO
```

Schema Registry usa RBAC (não ACL): `DeveloperWrite`/`DeveloperRead` para o producer e `DeveloperRead` para o consumer, no escopo `Subject:desafio*` (prefixo).

**Por que esta peça:** O cluster Basic atende à demanda de desenvolvimento com baixo custo horário e provisionamento em minutos. O isolamento de identidades por Service Accounts (`desafio-producer` e `desafio-consumer`) com ACLs prefixadas e RBAC no Schema Registry aplica estritamente o princípio do menor privilégio, impedindo que credenciais de aplicação acessem ou alterem recursos fora do domínio `desafio.*`.

## Camada 2 — Contrato de dados

Schemas Avro em `schemas/` sob o namespace `com.bootcamp.payments.*`, com tipos monetários tipados como `bytes` + `logicalType: decimal(15,2)` (precisão monetária exata sem erros de arredondamento de float/double) e tags de governança `PII` e `PCI` cadastradas no catálogo do Stream Governance.

Script de automação: `scripts/03-schemas.sh`. Evidência completa: `evidencias/03-contrato-de-dados.txt`.

### Schemas Registrados

```
$ confluent schema-registry schema list --subject-prefix desafio
    ID   |              Subject              | Version
---------+-----------------------------------+----------
  100001 | desafio.customers-value           |       1
  100002 | desafio.accounts-value            |       1
  100003 | desafio.cards-value               |       1
  100004 | desafio.merchants-value           |       1
  100005 | desafio.transactions-value        |       1
  100006 | desafio.fraud.detected-value      |       1
```

### Classificação e Tags de Governança (Stream Governance)

Tags registradas no catálogo REST (`/catalog/v1/types/tagdefs`) e vinculadas aos campos dos schemas:
- `PII`: `customers.full_name`, `customers.email`, `customers.phone`, `customers.birth_date`
- `PCI`: `cards.card_number_mask`

### Evolução de Esquema e Compatibilidade

Compatibilidade global ativa: `BACKWARD`.

**1. Aceitando campo novo com default (Evolução Compatível):**

```bash
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-compatible.avsc --type avro --subject desafio.test.evolution-value --version latest
+------------+------+
| Compatible | true |
+------------+------+
```

**2. Rejeitando campo obrigatório sem default em BACKWARD (Quebra de Compatibilidade):**

```bash
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-add-required.avsc --type avro --subject desafio.test.evolution-value --version latest
+------------+-------+
| Compatible | false |
+------------+-------+
```

**3. Rejeitando remoção de campo obrigatório em FULL (Quebra Bidirecional):**

```bash
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-incompatible.avsc --type avro --subject desafio.test.evolution-full-value --version latest
+------------+-------+
| Compatible | false |
+------------+-------+
```

**Por que esta peça:** O Schema Registry com Apache Avro estabelece contratos fortes entre produtores, Flink e consumidores. O formato binário Avro otimiza o uso de rede e armazenamento. A imposição de compatibilidade `BACKWARD` impede que mudanças inadvertidas quebrem aplicações consumidoras em produção e viabiliza deploys desacoplados (consumidores podem ser atualizados antes dos produtores).

## Camada 3 — Ingestão (CDC)

Conector gerenciado `PostgresCdcSourceV2` configurado em `connectors/cdc.json` (sem secrets, usando variáveis de ambiente), autenticado com a API key de `desafio-producer`, conectado com o usuário `cdc_user` (menor privilégio) e `heartbeat.interval.ms=60000`. Script de automação: `scripts/04-connector.sh`.

Seed determinístico carregado (`seed=42`, gerador em `db/seed.py`, saída idêntica a cada execução — sha256 `5e292bef…b136`). Saída completa em `evidencias/00-seed-e-banco.txt`:

```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
```

Preparação do Postgres (`./db/apply.sh`): `wal_level=logical`, `REPLICA IDENTITY FULL` nas 5 tabelas, publication `desafio_pub`, e `cdc_user` com `LOGIN REPLICATION` e estritamente `SELECT` (sem superuser, createdb, createrole nem permissão de escrita). O seed embute 6 cartões em rajada (3 a 5 transações em ≤ 40 s) que disparam a regra de fraude no Flink, mais 2 casos-controle que NÃO disparam (2 transações em 30 s; 3 transações em 90 s).

Status do conector na Confluent Cloud:

```
Connector Details
+--------+-------------+
| ID     | lcc-9ky0x6m |
| Name   | desafio-cdc |
| Status | RUNNING     |
| Type   | source      |
+--------+-------------+
```

### Envelopes CDC Capturados (Evidência Real)

Captura de ciclo completo `INSERT` $\rightarrow$ `UPDATE` $\rightarrow$ `DELETE` (`scripts/evidence-cdc.sh`). Arquivo completo em `evidencias/01-envelopes-cdc.json`:

**1. `op=c` (INSERT): `before=null`, `after` preenchido:**

```json
{
  "op": "c",
  "before": null,
  "after": {
    "desafio.public.customers.Value": {
      "birth_date": 8236,
      "city": "Sao Paulo",
      "created_at": "2026-11-23T17:11:32.497367Z",
      "customer_id": 99999,
      "email": "cdc.probe@bootcamp.com",
      "full_name": "CDC Probe User",
      "phone": "+5511999998888"
    }
  },
  "source": {
    "connector": "postgresql",
    "db": "neondb",
    "table": "customers",
    "snapshot": "false"
  }
}
```

**2. `op=u` (UPDATE): `before` e `after` completos (via `REPLICA IDENTITY FULL`):**

```json
{
  "op": "u",
  "before": {
    "desafio.public.customers.Value": {
      "birth_date": 8236,
      "city": "Sao Paulo",
      "created_at": "2026-11-23T17:11:32.497367Z",
      "customer_id": 99999,
      "email": "cdc.probe@bootcamp.com",
      "full_name": "CDC Probe User",
      "phone": "+5511999998888"
    }
  },
  "after": {
    "desafio.public.customers.Value": {
      "birth_date": 8236,
      "city": "Belo Horizonte",
      "created_at": "2026-11-23T17:11:32.497367Z",
      "customer_id": 99999,
      "email": "cdc.probe.updated@bootcamp.com",
      "full_name": "CDC Probe User",
      "phone": "+5511999998888"
    }
  },
  "source": {
    "connector": "postgresql",
    "db": "neondb",
    "table": "customers",
    "snapshot": "false"
  }
}
```

**3. `op=d` (DELETE): `before` presente e `after=null`:**

```json
{
  "op": "d",
  "before": {
    "desafio.public.customers.Value": {
      "birth_date": 8236,
      "city": "Belo Horizonte",
      "created_at": "2026-11-23T17:11:32.497367Z",
      "customer_id": 99999,
      "email": "cdc.probe.updated@bootcamp.com",
      "full_name": "CDC Probe User",
      "phone": "+5511999998888"
    }
  },
  "after": null,
  "source": {
    "connector": "postgresql",
    "db": "neondb",
    "table": "customers",
    "snapshot": "false"
  }
}
```

**4. Tombstone (mensagem com a mesma chave e valor nulo, logo após o `op=d`)**, lida do tópico com `--key-format avro --print-key`:

```json
{"key": {"customer_id": 99999}, "value": null}
```

**Por que esta peça:** O conector Debezium gerenciado (`PostgresCdcSourceV2`) lê o Write-Ahead Log (WAL) do Postgres de forma não intrusiva, sem impacto de polling de queries sobre o banco operacional. O `REPLICA IDENTITY FULL` garante que transições de estado (`before` e `after`) estejam presentes nos eventos de UPDATE e DELETE, permitindo que motores de stream como Flink SQL mantenham changelogs consistentes. O parâmetro `heartbeat.interval.ms=60000` avança periodicamente o LSN no slot de replicação mesmo sem novas transações de negócio, evitando acúmulo desnecessário de WAL no Neon e prevenindo estouro de armazenamento.

## Camada 4 — Processamento (Flink SQL)

Processamento de stream contínuo com Apache Flink gerenciado na Confluent Cloud (Compute Pool `desafio-pool`, região `aws/us-east-1`). Statements em `sql/` com a propriedade de execução `sql.tables.scan.idle-timeout=5s`. Motivo: todas as contas têm `updated_at = 2026-10-01 00:00:00` (conferido no Postgres: mín = máx) e o watermark de `desafio.accounts.versioned` é `updated_at - 5 s`, então fica parado em `2026-09-30 23:59:55` (derivado do DDL e dos dados). Sem o `idle-timeout`, o temporal join não emite nada (com `idle-timeout=0` o `enriched` ficou vazio, segundo o diário de execução; não foi remedido nesta sessão). Script de automação: `scripts/05-flink.sh`. Evidência completa: `evidencias/04-alerta-fraude.txt`.

**O caminho é o real, de ponta a ponta:** Postgres → conector CDC → `desafio.public.transactions` → Flink → `desafio.fraud.detected`. Nenhuma transação foi injetada à mão no Flink: os alertas abaixo nascem das 2000 transações do seed.

### Tabelas e tempo de evento (`scripts/05-flink.sh`)

A tabela que o conector publica chega ao Flink como *changelog* (`retract`) e com `txn_ts` em texto. Para o temporal join e o `MATCH_RECOGNIZE` o script faz três ajustes, todos por DDL:

1. `changelog.mode = 'append'` em `desafio.public.transactions` (a tabela só recebe INSERTs de negócio);
2. coluna calculada `txn_time` = `txn_ts` convertido para `TIMESTAMP_LTZ(3)`, com `WATERMARK ... INTERVAL '5' SECOND`. O relógio é o do **evento** (`txn_ts` do Postgres, em outubro/2026), não o de ingestão;
3. `desafio.accounts.versioned` com `PRIMARY KEY (account_id)` e `updated_at` como tempo de evento (exigência do `FOR SYSTEM_TIME AS OF`).

### Statements Flink SQL em execução

```
$ confluent flink statement list
  - desafio-accounts-populate [Status: RUNNING]
  - desafio-enrichment [Status: RUNNING]
  - desafio-fraud-detection [Status: RUNNING]
```

- `desafio-accounts-populate`: alimenta `desafio.accounts.versioned` (PK `account_id`) a partir de `desafio.public.accounts`.
- `desafio-enrichment` (`sql/01-enrichment.sql`): **temporal join** `transactions ⋈ accounts` com `FOR SYSTEM_TIME AS OF t.txn_time`, gravando em `desafio.transactions.enriched`.
  - **Corrida de partida, diagnosticada e corrigida.** Medido antes da correção: `desafio.transactions.enriched` com **1.042 de 2.000** transações, só as contas 1 a 122 (240 na origem). O `$rowtime` de `desafio.accounts.versioned` mostrou duas levas de gravação do `desafio-accounts-populate`: contas 1 a 122 às 17:04:46-47 e 123 a 240 às 17:04:52, separadas por ~5 s, igual ao `sql.tables.scan.idle-timeout=5s`. Mecanismo: todas as contas têm `updated_at = 2026-10-01 00:00:00`, então o watermark do lado versionado fica abaixo de qualquer transação e o temporal join só emite quando o `idle-timeout` libera as partições; nesse instante só a 1ª leva estava no estado, e o inner join descartou as transações das contas da 2ª leva (o join não reavalia linhas já processadas).
  - **Confirmação:** com o `desafio-enrichment` removido, a tabela `enriched` recriada com o mesmo DDL e o statement submetido de novo com `accounts.versioned` já completa (240 contas), o resultado foi **2.000 linhas / 2.000 transações distintas / 240 contas**, estável em duas medições.
  - **Correção no `scripts/05-flink.sh`:** depois do `desafio-accounts-populate`, a função `wait_accounts_versioned` espera a contagem de contas distintas em `accounts.versioned` igualar a de `accounts` no Postgres (timeout de 600 s, falha com mensagem clara) antes de subir `desafio-enrichment` e `desafio-fraud-detection`. A espera olha a contagem, não o status do populate, que é streaming e nunca termina.
- `desafio-fraud-detection` (`sql/02-fraud-detection.sql`): regra `RULE_VELOCITY`, gravando em `desafio.fraud.detected`.

### Regra de fraude: velocidade em tempo real (`RULE_VELOCITY`)

3 ou mais transações no mesmo cartão em menos de 60 segundos (`sql/02-fraud-detection.sql`):

```sql
INSERT INTO `desafio.fraud.detected` (
  card_id,
  account_id,
  customer_id,
  first_transaction_id,
  last_transaction_id,
  txn_count,
  total_amount,
  window_start,
  window_end,
  rule_name,
  alert_ts
)
SELECT
  m.card_id,
  m.account_id,
  a.customer_id,
  m.first_transaction_id,
  m.last_transaction_id,
  CAST(m.txn_count AS INT) AS txn_count,
  CAST(m.total_amount AS DECIMAL(15, 2)) AS total_amount,
  m.window_start,
  m.window_end,
  m.rule_name,
  m.alert_ts
FROM (
  SELECT card_id, account_id, first_transaction_id, last_transaction_id, txn_count, total_amount,
         window_start, window_end, rule_name, alert_ts
  FROM `desafio.public.transactions`
  MATCH_RECOGNIZE (
    PARTITION BY card_id
    ORDER BY txn_time
    MEASURES
      FIRST(T.account_id) AS account_id,
      FIRST(T.transaction_id) AS first_transaction_id,
      LAST(T.transaction_id) AS last_transaction_id,
      COUNT(T.transaction_id) AS txn_count,
      SUM(T.amount) AS total_amount,
      FIRST(T.txn_time) AS window_start,
      LAST(T.txn_time) AS window_end,
      CAST('RULE_VELOCITY_60S' AS STRING) AS rule_name,
      CURRENT_ROW_TIMESTAMP() AS alert_ts
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (T{3,}?)
    WITHIN INTERVAL '60' SECOND
    DEFINE
      T AS TRUE
  )
) m
JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF m.window_end AS a
  ON m.account_id = a.account_id;
```

**Por que `txn_count` é sempre 3.** O padrão `T{3,}?` é relutante: fecha o match assim que tem 3 linhas, e `AFTER MATCH SKIP PAST LAST ROW` descarta o que sobrar da rajada. Isso é visível nos dados: no Postgres o cartão 320 tem uma rajada de **4** transações (839 a 842, em 26 s) e o cartão 260 uma de **5** (1747 a 1751, em 26 s), mas cada um gerou **um** alerta com `txn_count=3` (839 a 841 e 1747 a 1749). Consequência: o alerta diz "houve 3 ou mais em 60 s", não "quantas houve".

**Decisão de projeto (aprendida na execução).** A primeira versão aplicava o `MATCH_RECOGNIZE` sobre o tópico `desafio.transactions.enriched`, isto é, depois do join. Com isso saíam só 3 dos 6 alertas esperados: os cartões 114, 228 e 320 ficavam sem alerta, embora suas transações estivessem corretas no `enriched`. Ao consultar `CURRENT_WATERMARK`, todas essas transações apareciam como **atrasadas** (o watermark da tabela já estava semanas à frente), então o Flink as descartava. A versão atual detecta o padrão **antes** do join e só depois busca o `customer_id` da conta (`FOR SYSTEM_TIME AS OF m.window_end`). Resultado: 6 de 6 rajadas do seed (cartões 100, 114, 228, 245, 260, 320), sem alerta para 126 e 86.

Por que isso é o desenho correto e não um contorno: o temporal join **não grava o `enriched` em ordem de event time**. Medido lendo o tópico na ordem dos offsets (`evidencias/07-ordem-enriched.txt`, ambiente com a espera nova): 1986 das 2000 linhas têm `txn_ts` abaixo do maior `txn_ts` já visto (maior atraso: 29 dias e 16 h); o `account_id` muda 240 vezes (um bloco por conta, 240 contas); o maior `txn_ts` já chega a 2026-10-29 no offset 10, isto é, o relógio de evento salta para o fim de outubro logo no início e quase todas as linhas seguintes chegam "atrasadas". Um `MATCH_RECOGNIZE ... ORDER BY txn_time` sobre esse tópico depende do watermark, e linhas atrasadas em relação a ele são descartadas (foi o que o `CURRENT_WATERMARK` mostrou no 1º ciclo). Já `desafio.public.transactions`, na ordem de `transaction_id`, não tem nenhuma linha com `txn_ts` abaixo do maior anterior (medido no Postgres: 0 de 2000; a ordem do tópico em si não foi medida). Isso é compatível com a perda dos cartões 114, 228 e 320 no 1º ciclo (incluindo o 114, que a corrida de partida não explicava). Limite desta afirmação: o `MATCH_RECOGNIZE` sobre o `enriched` **não foi reexecutado** nesta sessão, e os dados do 1º ciclo não existem mais; o que foi medido é a ordem de gravação, não a perda dos alertas.

### Alertas emitidos vs. transações de origem (evidência real)

**Alertas lidos do tópico `desafio.fraud.detected`** (`scripts/evidence-flink.sh`, 6 alertas, um por rajada do seed). O `total_amount` aparece como `"4651/10"` porque a CLI imprime o decimal Avro como fração (numerador/denominador): 4651/10 = **465,10**, que é a soma das 3 transações do cartão 100 (conferida no Postgres: 1726 a 1728, soma 465.10, n=3):

```json
{"card_id":100,"customer_id":100,"first_transaction_id":1726,"last_transaction_id":1728,"txn_count":3,"total_amount":"4651/10","window_start":"2026-10-26T10:14:04Z","window_end":"2026-10-26T10:14:26Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":114,"customer_id":114,"first_transaction_id":1216,"last_transaction_id":1218,"txn_count":3,"total_amount":"14187/25","window_start":"2026-10-19T06:48:07Z","window_end":"2026-10-19T06:48:17Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":245,"customer_id":160,"first_transaction_id":117,"last_transaction_id":119,"txn_count":3,"total_amount":"2811/20","window_start":"2026-10-03T07:51:57Z","window_end":"2026-10-03T07:52:28Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":320,"customer_id":9,"first_transaction_id":839,"last_transaction_id":841,"txn_count":3,"total_amount":"36763/100","window_start":"2026-10-14T01:14:27Z","window_end":"2026-10-14T01:14:46Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":260,"customer_id":111,"first_transaction_id":1747,"last_transaction_id":1749,"txn_count":3,"total_amount":"4153/20","window_start":"2026-10-26T19:30:49Z","window_end":"2026-10-26T19:31:07Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":228,"customer_id":63,"first_transaction_id":882,"last_transaction_id":884,"txn_count":3,"total_amount":"1618/25","window_start":"2026-10-14T14:34:23Z","window_end":"2026-10-14T14:34:59Z","rule_name":"RULE_VELOCITY_60S"}
```

**Transações de origem do alerta do cartão 100** (consulta real ao Postgres; as demais estão em `evidencias/04-alerta-fraude.txt`):

```
--- card_id=100 (transações 1726 a 1728)
 transaction_id | card_id | amount |  status  |         txn_ts         
----------------+---------+--------+----------+------------------------
           1726 |     100 | 193.20 | APPROVED | 2026-10-26 10:14:04+00
           1727 |     100 |  36.29 | APPROVED | 2026-10-26 10:14:23+00
           1728 |     100 | 235.61 | APPROVED | 2026-10-26 10:14:26+00
(3 rows)
```

Os três eventos do cartão 100 ocorrem em 22 s (10:14:04 a 10:14:26), dentro da janela de 60 s.

**Controle negativo:** Nenhum alerta para os cartões 126 e 86, como esperado.

**Por que esta peça:** O Apache Flink SQL na Confluent Cloud permite declarar lógica complexa de processamento de eventos (CEP via `MATCH_RECOGNIZE` e Temporal Joins) em SQL padrão declarativo, sem a necessidade de manter e orquestrar microsserviços customizados com estado em memória (como Redis ou um banco de estado externo). O Flink gerencia o estado das janelas com semântica *exactly-once* e tolerância a falhas nativa, permitindo detecção de fraude diretamente sobre o fluxo de streaming do Kafka.

## Camada 5 — Consumo e Operação (Os Quatro Pilares)

### Consumo e Menor Privilégio (`desafio-consumer`)

A leitura dos alertas de fraude é realizada com a service account `desafio-consumer`, que possui apenas permissões de leitura no tópico `desafio.fraud.detected` e no Schema Registry. Script de consumo: `scripts/consume-alerts.sh` e `scripts/06-consume.sh`. Evidência completa: `evidencias/05-consumo.txt`.

**1. Leitura de Alertas (Autorizada):**

```bash
$ confluent kafka topic consume desafio.fraud.detected \
    --group desafio-alerts-client \
    --value-format avro --schema-registry-endpoint <SR_URL>
```

```json
{
  "account_id": 214,
  "alert_ts": "2026-11-23T17:06:00.334Z",
  "card_id": 320,
  "customer_id": 9,
  "first_transaction_id": 839,
  "last_transaction_id": 841,
  "rule_name": "RULE_VELOCITY_60S",
  "total_amount": "36763/100",
  "txn_count": 3,
  "window_end": "2026-10-14T01:14:46Z",
  "window_start": "2026-10-14T01:14:27Z"
}
```

**2. Teste Negativo de Escrita (Comprovação de Menor Privilégio):**
Tentativa do `desafio-consumer` de produzir mensagens em tópicos do cluster:

```bash
$ echo '{"attempt": "unauthorized_write_test"}' | confluent kafka topic produce desafio.fraud.detected
Starting Kafka Producer. Use Ctrl-C or Ctrl-D to exit.
failed to produce offset -1: Broker: Topic authorization failed
```
*Acesso negado estritamente pelas ACLs do Kafka, provando que o consumidor não tem capacidade de poluir dados.*

---

### As Quatro Respostas Operacionais (Evidências Reais)

Evidência completa gerada por `scripts/07-operation.sh` em `evidencias/06-operacao.txt`:

#### ① Observabilidade (Métricas e Lag)

Medições reais coletadas da **Confluent Telemetry Metrics API** (`https://api.telemetry.confluent.cloud/v2/metrics/cloud/query` via `scripts/metrics.sh`), granularidade de 1 h, janela de 2026-11-16 a 2026-11-23 (`METRICS_START=2026-11-16T00:00:00Z ./scripts/metrics.sh`):

- **Pico de ingestão (`io.confluent.kafka.server/received_bytes`):** `1.084.678` bytes (~1,08 MB) em 2026-11-17 13:00 UTC, na carga inicial (*snapshot*) do CDC.
- **Pico de egress (`io.confluent.kafka.server/sent_bytes`):** `7.019.047` bytes (~7,02 MB) em 2026-11-23 13:00 UTC, quando os statements Flink foram recriados e releram os tópicos desde o início (`earliest-offset`).
- **Armazenamento retido (`io.confluent.kafka.server/retained_bytes`):** pico de `1.666.322` bytes (~1,67 MB); `1.386.533` bytes na última leitura.
- **Lag por grupo (`confluent kafka consumer group list`):** **não medido.** Os grupos de consumo desta entrega são efêmeros (cada leitura de evidência usa um `--group` novo e termina), então a CLI devolve `None found` e não há lag a reportar. Em produção, o lag do consumidor de alertas viria da métrica `io.confluent.kafka.server/consumer_lag_offsets` por `consumer_group_id`.

#### ② Segurança (Matriz de Menor Privilégio)

| Identidade / SA | Escopo de Recurso | Permissões / Operações | Justificativa Técnica |
|---|---|---|---|
| `desafio-producer` | Tópicos `desafio*`, `__debezium-heartbeat*` | `CREATE`, `WRITE`, `DESCRIBE` | Permite ao conector CDC e Flink Statements produzir eventos nos tópicos e publicar heartbeats periódicos. |
| `desafio-producer` | Cluster Kafka (`kafka-cluster`) | `DESCRIBE`, `DESCRIBE_CONFIGS` | Exigido pelo Debezium V2 para inspecionar partições e metadados do broker. |
| `desafio-producer` | Schema Registry (`Subject:desafio*`) | `DeveloperWrite`, `DeveloperRead`, `DataSteward` | Registro de novos schemas Avro e aplicação de tags de catálogo (`PII`, `PCI`). |
| `desafio-consumer` | Tópicos `desafio*` | `READ`, `DESCRIBE` | Leitura de alertas e streams de negócio por microsserviços de downstream. |
| `desafio-consumer` | Consumer Groups `desafio-*` | `READ` | Entrada em grupos de consumo gerenciados. |
| `desafio-consumer` | Schema Registry (`Subject:desafio*`) | `DeveloperRead` | Deserialização de mensagens Avro sem permissão de mutação de schemas. |
| `cdc_user` (Postgres) | Banco `neondb` (Schema `public`) | `LOGIN`, `REPLICATION`, `SELECT ON ALL TABLES` | Leitura do WAL e da publication `desafio_pub` sem privilégios de DDL, escrita ou superuser. |

#### ③ Custos Reais e Alavancas de Otimização

Consumo real medido com `confluent billing cost list --start-date 2026-11-01 --end-date 2026-11-24 -o json` (`scripts/cost.sh`), período de 2026-11-16 a 2026-11-24, somando as linhas por tipo:

| Item | Consumo bruto | % do total |
|---|---|---|
| Flink (CFU-hora) — `desafio-pool` | US$ 93.53 | 49.4% |
| Conector CDC (task-hora) — `desafio-cdc` | US$ 72.50 | 38.3% |
| Cluster Kafka Basic (CKU-hora) — `desafio-basic` | US$ 23.22 | 12.3% |
| Rede (escrita) | US$ 0.07 | 0.0% |
| Rede (leitura) | US$ 0.02 | 0.0% |
| **Total bruto** | **US$ 189.33** | 100% |
| Crédito promocional `FREETRIAL400` | US$ -189.33 | |
| **Líquido cobrado no cartão** | **US$ 0.00** | |

*O que gera custo é o tempo ligado, não o volume de dados: tráfego de rede somou centavos. Flink e conector respondem por cerca de 88% do total e cobram por hora enquanto existirem, com ou sem mensagens. O ritmo com tudo ligado ficou em torno de US$ 30 a 35 por dia.*

**Duas alavancas de redução, escolhidas pelos números acima:**
1. **Derrubar o que não está em uso (`./teardown.sh`).** Flink + conector + cluster não têm "pausa": só apagar interrompe a cobrança. Entre sessões de trabalho, o teardown elimina cerca de 100% do custo de hora; o `./setup.sh` recria tudo em poucos minutos.
2. **Dimensionar o Flink ao mínimo e não manter statements sem consumidor.** O pool tem teto de 5 CFU (`--max-cfu 5`) e cada statement ativo consome CFU. Durante o desenvolvimento esta entrega chegou a ter um 4º statement rodando sem que nada lesse o seu resultado (removido); manter só os statements com consumidor, e o pool no menor teto que sustenta a carga, reduz a maior linha da fatura.

#### ④ Confiabilidade Fim a Fim e Idempotência

| Trecho do Pipeline | Garantia de Entrega | Mecanismo de Tolerância a Falhas |
|---|---|---|
| **Postgres (WAL) $\rightarrow$ CDC Debezium** | *At-least-once* | Replication slot com LSN confirmado após ACK no Kafka. **Ponto frágil, observado na prática:** se o slot some no Postgres (WAL descartado ou slot recriado), o conector vai para `FAILED` (`USER_ACTIONABLE_ERROR`) e não retoma sozinho. Por isso `connectors/cdc.json` usa `snapshot.mode=when_needed`; ver "Teste de recuperação" abaixo. |
| **CDC $\rightarrow$ Tópicos Kafka** | *At-least-once* | Tópicos com $RF=3$ (medido com `confluent kafka topic describe`). A chave de cada mensagem é a PK da tabela (`transaction_id` em `transactions`, `account_id` em `accounts`, `customer_id`, `card_id` e `merchant_id` nas demais; conferido nos schemas `*-key`). Os tópicos do CDC têm **1 partição**, então a ordem por tópico é total e a chave não distribui carga. |
| **Kafka $\rightarrow$ Flink SQL $\rightarrow$ Tópicos Alerta** | *Exactly-once* | Checkpoints periódicos de estado e transações Kafka no sink Flink. |
| **Tópico de Alerta $\rightarrow$ Consumidor Final** | *At-least-once* | **[ELO FRACO]**: A entrega da mensagem é garantida, mas o efeito externo (bloqueio do cartão, envio de notificação) exige controle de **idempotência na aplicação consumidora** chaveado por `(card_id, first_transaction_id, window_start)` para prevenir duplicação em rebalances. |

## Como subir, parar e retomar

```bash
./setup.sh                    # sobe/retoma tudo, na ordem (idempotente: pode rodar de novo)
./scripts/status.sh           # o que existe agora (e portanto gera custo) — somente leitura
./teardown.sh --dry-run       # mostra o que seria apagado, sem apagar nada
./teardown.sh                 # apaga (pede para digitar "apagar")
./teardown.sh --yes           # apaga sem perguntar
./teardown.sh --keep-neon     # não mexe no slot de replicação do Postgres
```

**Parar de gastar (pausa entre sessões).** O cluster Kafka Basic, o conector e o Flink cobram por hora enquanto existirem; não há "pausar", só apagar. `./teardown.sh` apaga, nesta ordem: conectores → Flink (statements e compute pool) → role bindings do Schema Registry → API keys → cluster → service accounts → environment (leva o Schema Registry junto) → slot de replicação no Postgres. Ele só toca nos recursos com os nomes do `.env` e **nunca** no environment `default`. Ao fim imprime as listas para provar que nada restou.

**Retomar.** `./setup.sh` recria tudo a partir do zero (leva alguns minutos) e roda cada `scripts/NN-*.sh` em ordem. Como o teardown remove o cluster, **os tópicos, schemas registrados e dados do Kafka são perdidos e recriados**; o conector refaz o snapshot do Postgres. Os dados de origem no Neon sobrevivem ao teardown (o `setup.sh` só recarrega o seed se faltarem linhas; `./setup.sh --reseed` força).

**O que o teardown NÃO apaga:** o projeto Neon e seus dados, o `.env`, e os arquivos em `evidencias/`. Para apagar o projeto Neon: `neon projects delete <id>` (veja o ID em `.neon`). A logical replication da Neon é irreversível, mas isso só afeta aquele projeto.

**Segredos:** as API keys ficam em `.state` (gitignored). O teardown apaga o `.state` junto com as keys; o `setup.sh` gera keys novas.

### Teste de recuperação do conector (slot derrubado de propósito)

Na primeira sessão o conector `desafio-cdc` terminou `FAILED` com o slot de replicação ausente no Postgres (causa do sumiço do slot **não identificada**). A mensagem do conector sugeria `snapshot.mode=when_needed`. Para verificar, o conector foi atualizado (`confluent connect cluster update --config snapshot.mode=when_needed`) e o slot `desafio_cdc_slot` foi derrubado de propósito com o conector ligado (`pg_drop_replication_slot`). Saída observada (estado do conector | slot no Postgres):

```
[antes]      {"c":"RUNNING","t":["RUNNING"]} | desafio_cdc_slot active=true
[slot caiu]  SLOT AUSENTE
[+30s]       {"c":"FAILED","t":["USER_ACTIONABLE_ERROR"]} | SLOT AUSENTE
[+60s]       {"c":"FAILED","t":["USER_ACTIONABLE_ERROR"]} | desafio_cdc_slot active=false
[+120s]      {"c":"FAILED","t":["USER_ACTIONABLE_ERROR"]} | desafio_cdc_slot active=true
[+180s]      {"c":"RUNNING","t":["RUNNING"]} | desafio_cdc_slot active=true
[+240s]      {"c":"RUNNING","t":["RUNNING"]} | desafio_cdc_slot active=true
```

Com `when_needed` o conector se recuperou sozinho em cerca de 3 minutos (recriou o slot e refez o snapshot). Depois disso um novo ciclo INSERT/UPDATE/DELETE voltou a chegar ao Kafka (`op=c`, `u`, `d` e tombstone). Sem essa opção, o mesmo cenário deixa o conector em `FAILED` até intervenção manual. Como o snapshot é refeito, o consumidor pode ver registros repetidos: a idempotência continua sendo responsabilidade da aplicação (ver tabela acima).

### Pendências e limites conhecidos desta entrega

- **Ainda não provado:** um `./setup.sh` limpo, de ponta a ponta, numa única execução, com a espera `wait_accounts_versioned` ativa. A correção foi testada com o ambiente já no ar (caminho feliz 240/240 e caminho de falha por timeout), mas não no cenário em que a corrida ocorria (populate e enrichment subindo quase juntos).
- **Causas não identificadas:** o sumiço do slot de replicação na 1ª sessão; a tabela `desafio.accounts.versioned` que terminava o DDL como `COMPLETED` sem aparecer no catálogo do Flink (2 ciclos; o script detecta e recria); a falha única de criação do conector ("failed to connect with Schema Registry").
- **Alertas do 1º ciclo (cartões 114, 228 e 320):** a corrida de partida **não** explica a perda: as transações desses cartões estavam no `enriched` e o 114 é da 1ª leva. O que se mediu (`evidencias/07-ordem-enriched.txt`) é que o temporal join grava o `enriched` fora de ordem de event time: 1986 de 2000 linhas "atrasadas", 240 blocos de `account_id`, maior `txn_ts` já em 2026-10-29 no offset 10. Isso é compatível com a perda, mas **não foi reproduzido** (o `MATCH_RECOGNIZE` sobre o `enriched` não foi reexecutado e os dados do 1º ciclo não existem mais).
- **`desafio.transactions.enriched`** não é lido por nenhum consumidor; a regra de fraude lê `desafio.public.transactions` direto. Qualquer consumidor do `enriched` por event time (janela, `MATCH_RECOGNIZE`, join temporal) sofre do mesmo problema de gravação fora de ordem.

### Problemas encontrados e corrigidos no `setup.sh` (execução do zero)

Rodar `./setup.sh` do zero expôs 6 falhas, todas corrigidas e reverificadas até o `setup.sh` terminar com `exit=0` e `./scripts/status.sh` mostrar conector `RUNNING` e 3 statements Flink `RUNNING`:

1. `01-foundation.sh`: a API REST do cluster recém-criado recusava conexões por mais de 30 s ("connection reset by peer") e o `retry` desistia. O retry das ACLs passou de 6 para 36 tentativas.
2. `03-schemas.sh`: a criação das tags PII/PCI falhava logo após criar o role binding `DataSteward` (propagação). Passou a usar `retry`.
3. `04-connector.sh`: lia o status no JSON errado (`.status`, quando o CLI devolve `.connector.status` e `.tasks[].state`) e ficava esperando sem enxergar o estado. Corrigido. Também recria o conector (até 3 vezes) se ele falhar na criação, o que ocorreu uma vez com "failed to connect with Schema Registry" logo após criar as API keys.
4. `05-flink.sh`: os DDLs engoliam qualquer erro (`>/dev/null 2>&1 || true`) e o script anunciava "operacional" com os 3 statements em `FAILED`. Agora cada DDL e cada statement têm o status verificado e o script aborta se não ficarem `COMPLETED`/`RUNNING`.
5. `05-flink.sh`: o `CREATE TABLE` de `desafio.accounts.versioned` terminava `COMPLETED` e deixava só o tópico e os subjects, **sem a tabela no catálogo do Flink** (observado em 2 ciclos). A causa não foi identificada. O script agora confere com `SHOW CREATE TABLE`, apaga os restos e recria. Funcionou na terceira subida.

6. `evidence-cdc.sh` não capturava o tombstone (`tombstone: null` na evidência anterior, apesar de o README dizer que ele existia). Passou a ler a chave (`--print-key`) e a registrar o tombstone.


## Custo total e teardown

> **Estado desta seção:** os números e a saída de teardown abaixo são do **primeiro ciclo** (2026-11-23, antes das correções acima). Depois disso o pipeline foi recriado do zero com `./setup.sh` para validar as correções. O custo final e a verificação de listas vazias do ciclo definitivo devem ser refeitos depois do último `./teardown.sh`, com `./scripts/cost.sh` consultado depois dele.

**Custo declarado:** consumo bruto de **US$ 189.33** no período 2026-11-16 a 2026-11-24, **totalmente coberto** pelo crédito promocional `FREETRIAL400` (US$ -189.33), ou seja, **US$ 0.00 cobrados no cartão**. Detalhamento por item no pilar ③ acima. O valor só cresceria com o ambiente ligado; este é o valor da consulta feita **depois** do `./teardown.sh` (2026-11-23, `./scripts/cost.sh 2026-11-01 2026-11-24`), somando as linhas do `confluent billing cost list -o json`: Flink 93.53 + conector 72.50 + cluster 23.22 + rede 0.085 = 189.33, contra créditos de -189.33 (líquido US$ 0.00). Como o billing tem defasagem, a última linha diária (2026-11-23) pode ainda receber ajustes; o ambiente já não existe, então nada novo é gerado.

```bash
$ ./scripts/cost.sh    # confluent billing cost list --start-date ... --end-date ...
```

### Verificação de Listas Vazias pós-Teardown

Saída real de `./teardown.sh` (executado em 2026-11-23 com o conector `desafio-cdc` em `FAILED` — o slot de replicação já havia sumido no Postgres — depois de `./teardown.sh --dry-run`). Ordem reversa de dependência; `None found.` nas listas de conectores e statements Flink, só o environment `default` restante e nenhuma service account:

```
== O que será apagado
   environment : env-6nj08j (desafio-final)
   cluster     : lkc-yow5gwo (desafio-basic)
   Sch.Registry: lsrc-w7w0zw5 (some junto com o environment)
   conectores  : lcc-q2wnnv6
   flink stmts : tmp-desc-21970
tmp-desc-25547
tmp-desc-956
tmp-q-303846252
tmp-q-1640715298
desafio-accounts-populate
desafio-enrichment
desafio-fraud-detection
   flink pools : lfcp-w7wpx0g
   service acc.:  sa-7p15z9p sa-o27kynx
   slot Neon   : slots 'desafio%' inativos
   .state local: será removido (as API keys morrem junto)

(confirmação "apagar" via stdin)
>> 1/8 conectores
   ok: confluent connect cluster delete lcc-q2wnnv6 --cluster lkc-yow5gwo --environment env-6nj08j --force
>> 2/8 Flink statements e compute pool
   ok: confluent flink statement delete tmp-desc-21970 --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete tmp-desc-25547 --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete tmp-desc-956 --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete tmp-q-303846252 --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete tmp-q-1640715298 --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete desafio-accounts-populate --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete desafio-enrichment --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink statement delete desafio-fraud-detection --cloud aws --region us-east-1 --environment env-6nj08j --force
   ok: confluent flink compute-pool delete lfcp-w7wpx0g --environment env-6nj08j --force
   -- verificação (esperado: None found.)
   connect cluster list:
     None found.
   flink statement list:
     None found.
>> 3/8 role bindings do Schema Registry
   ok: confluent iam rbac role-binding delete --principal User:sa-7p15z9p --role DeveloperWrite --environment env-6nj08j --schema-registry-cluster lsrc-w7w0zw5 --resource Subject:desafio --prefix --force
   ok: confluent iam rbac role-binding delete --principal User:sa-7p15z9p --role DeveloperRead --environment env-6nj08j --schema-registry-cluster lsrc-w7w0zw5 --resource Subject:desafio --prefix --force
   ok: confluent iam rbac role-binding delete --principal User:sa-o27kynx --role DeveloperRead --environment env-6nj08j --schema-registry-cluster lsrc-w7w0zw5 --resource Subject:desafio --prefix --force
   ok: confluent iam rbac role-binding delete --principal User:sa-7p15z9p --role DataSteward --environment env-6nj08j --force
>> 4/8 API keys
   ok: confluent api-key delete O2ILJ2HWWSESPKSI --force
   ok: confluent api-key delete ZHNOYEEVFTZ47LCW --force
   ok: confluent api-key delete J2ZZZNGLBLJP2JSD --force
   ok: confluent api-key delete UN5DOIUPSVM2GQBC --force
>> 5/8 cluster Kafka
   ok: confluent kafka cluster delete lkc-yow5gwo --environment env-6nj08j --force
>> 6/8 service accounts
   ok: confluent iam service-account delete sa-7p15z9p --force
   ok: confluent iam service-account delete sa-o27kynx --force
>> 7/8 environment
   ok: confluent environment delete env-6nj08j --force
>> 8/8 slot de replicação no Postgres
   slots 'desafio%' restantes: 0

== Verificação final (nada com o prefixo 'desafio-' deve restar)
-- confluent environment list
     Current |     ID     |  Name   | Stream Governance Package  
   ----------+------------+---------+----------------------------
             | env-2kpg72 | default | ESSENTIALS                 
-- confluent iam service-account list
   None found.

Pronto. Para retomar: ./setup.sh
```

Conferência posterior com `./scripts/status.sh` (somente leitura):

```
== Confluent Cloud
   environment desafio-final : NÃO EXISTE  -> nada gerando custo na Confluent
   service accounts  : nenhuma
== Estado local
   .state ausente (setup ainda não rodou, ou o teardown já apagou)
== Neon (Postgres de origem)
   transactions: 2000
   nenhum slot de replicação 'desafio%'
```
