# Desafio Final — Pipeline de Pagamentos End-to-End

Pipeline de streaming de pagamentos em tempo real:

**Neon (Postgres)** → **CDC Debezium (`PostgresCdcSourceV2`)** → **tópicos Avro + Schema Registry (tags PII/PCI)** → **Flink SQL (enriquecimento + detecção de fraude)** → **consumo com menor privilégio**.

Reproduzível com `./setup.sh` (pré-requisito: `.env` preenchido a partir do `.env.example`).

## Pré-requisitos

| Item | Como obter |
|---|---|
| Conta Confluent Cloud | <https://confluent.cloud/signup> |
| Conta Neon (Postgres) | <https://console.neon.tech>. O CDC exige logical replication: `neon projects update <id> --enable-logical-replication --yes` (**irreversível** para o projeto; o `db/apply.sh` aborta se `wal_level` não for `logical`) |
| Confluent CLI | macOS/Linux: `brew install confluentinc/tap/cli`. Outros sistemas: <https://docs.confluent.io/confluent-cli/current/install.html>. Comandos verificados na `v4.70.0` |
| `psql` | macOS: `brew install libpq` e adicionar `/opt/homebrew/opt/libpq/bin` (Intel: `/usr/local/opt/libpq/bin`) ao `PATH`. Linux: `sudo apt install postgresql-client` |
| Python 3 | Gerador de dados `db/seed.py` (só biblioteca padrão; testado com 3.14.5) |
| `jq` | Usado pelos scripts |

Configuração:

```bash
cp .env.example .env
# edite o .env com suas credenciais; ele está no .gitignore e nunca deve ser versionado
```

Login na Confluent sem interação: `confluent login` aceita `CONFLUENT_CLOUD_EMAIL` e `CONFLUENT_CLOUD_PASSWORD`. Contas com SSO precisam rodar `confluent login` manualmente antes do `setup.sh`.

## Estrutura do repositório

```
.env.example
README.md
DOCUMENTACAO.md  # guia técnico: nomes, tópicos, schemas, quem liga em quem
setup.sh         # sobe/retoma tudo (idempotente)
teardown.sh      # derruba tudo (--dry-run para simular)
db/              # DDL + gerador determinístico seed=42 + cdc_user + apply.sh
schemas/         # Avro (com.bootcamp.payments.*)
connectors/      # cdc.json
sql/             # statements Flink SQL
scripts/         # lib.sh, NN-*.sh (uma por camada), status.sh, evidence-*.sh
evidencias/      # saídas capturadas
```

## Camada 1 — Fundação

Environment `desafio-final` + cluster Basic `desafio-basic` (aws/us-east-1). O Schema Registry nasce junto com o primeiro cluster do environment.

Identidades: `desafio-producer` (WRITE em `desafio*`) e `desafio-consumer` (READ em `desafio*` + grupo `desafio-*`). A conta pessoal só é usada em setup/teardown. Script: `scripts/01-foundation.sh`. Evidência completa: `evidencias/02-fundacao.txt`.

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

O Schema Registry usa RBAC (não ACL): `DeveloperWrite`/`DeveloperRead` para o producer e `DeveloperRead` para o consumer, no escopo `Subject:desafio*`.

**Por que esta peça:** o cluster Basic atende o desenvolvimento com baixo custo por hora e sobe em minutos. Duas service accounts com ACLs por prefixo e RBAC no Schema Registry aplicam o menor privilégio: uma credencial de aplicação não acessa nada fora de `desafio.*`.

## Camada 2 — Contrato de dados

Schemas Avro em `schemas/`, namespace `com.bootcamp.payments.*`. Valores monetários são `bytes` + `logicalType: decimal(15,2)` (precisão exata, sem erro de ponto flutuante). As tags `PII` e `PCI` estão cadastradas no catálogo do Stream Governance. Script: `scripts/03-schemas.sh`. Evidência completa: `evidencias/03-contrato-de-dados.txt`.

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

Tags de governança (catálogo `/catalog/v1/types/tagdefs`):
- `PII`: `customers.full_name`, `customers.email`, `customers.phone`, `customers.birth_date`
- `PCI`: `cards.card_number_mask`

### Evolução de esquema (compatibilidade global `BACKWARD`)

**1. Campo novo com default: aceito**

```bash
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-compatible.avsc --type avro --subject desafio.test.evolution-value --version latest
+------------+------+
| Compatible | true |
+------------+------+
```

**2. Campo obrigatório sem default: rejeitado em BACKWARD**

```bash
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-add-required.avsc --type avro --subject desafio.test.evolution-value --version latest
+------------+-------+
| Compatible | false |
+------------+-------+
```

**3. Remoção de campo obrigatório: rejeitada em FULL**

```bash
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-incompatible.avsc --type avro --subject desafio.test.evolution-full-value --version latest
+------------+-------+
| Compatible | false |
+------------+-------+
```

**Por que esta peça:** o Schema Registry com Avro cria contratos fortes entre produtores, Flink e consumidores, e o formato binário economiza rede e armazenamento. A compatibilidade `BACKWARD` impede que uma mudança quebre consumidores e permite atualizar consumidores antes dos produtores.

## Camada 3 — Ingestão (CDC)

Conector gerenciado `PostgresCdcSourceV2`, configurado em `connectors/cdc.json` (sem secrets, só variáveis de ambiente). Ele usa a API key do `desafio-producer`, conecta com o `cdc_user` (menor privilégio) e tem `heartbeat.interval.ms=60000`. Script: `scripts/04-connector.sh`.

Seed determinístico (`seed=42`, `db/seed.py`; saída idêntica a cada execução, sha256 `5e292bef…b136`). Saída completa em `evidencias/00-seed-e-banco.txt`:

```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
```

Preparação do Postgres (`./db/apply.sh`): `wal_level=logical`, `REPLICA IDENTITY FULL` nas 5 tabelas, publication `desafio_pub` e `cdc_user` com `LOGIN REPLICATION` e só `SELECT` (sem superuser, createdb, createrole nem escrita). O seed embute 6 cartões em rajada (3 a 5 transações em ≤ 40 s) que disparam a regra de fraude e 2 casos-controle que **não** disparam (2 transações em 30 s; 3 transações em 90 s).

```
Connector Details
+--------+-------------+
| ID     | lcc-9ky0x6m |
| Name   | desafio-cdc |
| Status | RUNNING     |
| Type   | source      |
+--------+-------------+
```

### Os três tipos de evento capturados

Ciclo `INSERT` → `UPDATE` → `DELETE` (`scripts/evidence-cdc.sh`). Aqui só os campos principais; os envelopes completos estão em `evidencias/01-envelopes-cdc.json`.

**`op=c` (INSERT):** `before=null` e `after` com a linha nova.

```json
{
  "op": "c",
  "before": null,
  "after": { "desafio.public.customers.Value": { "customer_id": 99999, "city": "Sao Paulo", "email": "cdc.probe@bootcamp.com" } },
  "source": { "connector": "postgresql", "db": "neondb", "table": "customers", "snapshot": "false" }
}
```

**`op=u` (UPDATE):** `before` e `after` completos (por causa do `REPLICA IDENTITY FULL`).

```json
{
  "op": "u",
  "before": { "desafio.public.customers.Value": { "customer_id": 99999, "city": "Sao Paulo",      "email": "cdc.probe@bootcamp.com" } },
  "after":  { "desafio.public.customers.Value": { "customer_id": 99999, "city": "Belo Horizonte", "email": "cdc.probe.updated@bootcamp.com" } },
  "source": { "connector": "postgresql", "db": "neondb", "table": "customers", "snapshot": "false" }
}
```

**`op=d` (DELETE):** `before` presente e `after=null`.

```json
{
  "op": "d",
  "before": { "desafio.public.customers.Value": { "customer_id": 99999, "city": "Belo Horizonte", "email": "cdc.probe.updated@bootcamp.com" } },
  "after": null,
  "source": { "connector": "postgresql", "db": "neondb", "table": "customers", "snapshot": "false" }
}
```

**Tombstone** (mesma chave e valor nulo, logo depois do `op=d`), lido com `--key-format avro --print-key`:

```json
{"key": {"customer_id": 99999}, "value": null}
```

**Por que esta peça:** o Debezium lê o WAL do Postgres sem consultar as tabelas, então não pesa no banco operacional. O `REPLICA IDENTITY FULL` entrega `before` e `after` em UPDATE e DELETE, o que o Flink precisa para manter changelogs corretos. O heartbeat avança o LSN do slot mesmo sem tráfego, evitando acúmulo de WAL no Neon.

## Camada 4 — Processamento (Flink SQL)

Flink gerenciado na Confluent Cloud (compute pool `desafio-pool`, aws/us-east-1). Statements em `sql/`, executados com `sql.tables.scan.idle-timeout=5s`. Script: `scripts/05-flink.sh`. Evidência completa: `evidencias/04-alerta-fraude.txt`.

O caminho é o real, de ponta a ponta: Postgres → conector → `desafio.public.transactions` → Flink → `desafio.fraud.detected`. Os alertas nascem das 2000 transações do seed.

**Por que o `idle-timeout`:** todas as contas têm `updated_at = 2026-10-01 00:00:00`, então o watermark de `desafio.accounts.versioned` (`updated_at - 5 s`) fica parado. Sem `idle-timeout`, o temporal join não emite nada.

### Tabelas e tempo de evento

A tabela publicada pelo conector chega ao Flink como changelog e com `txn_ts` em texto. O `scripts/05-flink.sh` faz três ajustes por DDL:

1. `changelog.mode = 'append'` em `desafio.public.transactions` (só recebe INSERTs de negócio);
2. coluna calculada `txn_time` = `txn_ts` convertido para `TIMESTAMP_LTZ(3)`, com `WATERMARK ... INTERVAL '5' SECOND`: o relógio é o do **evento**, não o de ingestão;
3. `desafio.accounts.versioned` com `PRIMARY KEY (account_id)` e `updated_at` como tempo de evento (exigência do `FOR SYSTEM_TIME AS OF`).

### Statements em execução

```
$ confluent flink statement list
  - desafio-accounts-populate [Status: RUNNING]
  - desafio-enrichment [Status: RUNNING]
  - desafio-fraud-detection [Status: RUNNING]
```

- `desafio-accounts-populate`: alimenta `desafio.accounts.versioned` a partir de `desafio.public.accounts`.
- `desafio-enrichment` (`sql/01-enrichment.sql`): temporal join `transactions ⋈ accounts` com `FOR SYSTEM_TIME AS OF t.txn_time`, gravando em `desafio.transactions.enriched`. O script só sobe este statement depois que `accounts.versioned` tem todas as 240 contas (função `wait_accounts_versioned`).
- `desafio-fraud-detection` (`sql/02-fraud-detection.sql`): regra `RULE_VELOCITY`, gravando em `desafio.fraud.detected`.

Resultado medido: `desafio.transactions.enriched` com **2000 linhas / 2000 transações distintas / 240 contas**.

### Regra de fraude: `RULE_VELOCITY`

3 ou mais transações no mesmo cartão em até 60 segundos. Núcleo do `sql/02-fraud-detection.sql`:

```sql
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY txn_time
  MEASURES ...
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?)
  WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
)
-- depois, temporal join com a conta para obter o customer_id:
JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF m.window_end AS a
  ON m.account_id = a.account_id
```

Dois pontos de projeto:

- **O padrão roda antes do join.** O `enriched` é gravado fora da ordem de `txn_ts` (medido: 1986 de 2000 linhas chegam "atrasadas"; `evidencias/07-ordem-enriched.txt`), então reconhecer o padrão sobre ele depende de watermark e descarta linhas. Por isso o `MATCH_RECOGNIZE` lê `desafio.public.transactions` direto e só depois busca o `customer_id`.
- **`txn_count` é sempre 3.** O padrão `T{3,}?` fecha o match na 3ª linha e `SKIP PAST LAST ROW` descarta o resto da rajada. Exemplo: o cartão 320 tem 4 transações em sequência e o cartão 260 tem 5, e cada um gerou um alerta com `txn_count=3`. O alerta diz "houve 3 ou mais em 60 s", não "quantas".

### Alertas emitidos vs. transações de origem

Alertas lidos de `desafio.fraud.detected` (`scripts/evidence-flink.sh`): **6 alertas**, um por rajada do seed. O `total_amount` aparece como `"4651/10"` porque a CLI imprime o decimal Avro como fração: 4651/10 = **465,10**, a soma das 3 transações do cartão 100.

```json
{"card_id":100,"customer_id":100,"first_transaction_id":1726,"last_transaction_id":1728,"txn_count":3,"total_amount":"4651/10","window_start":"2026-10-26T10:14:04Z","window_end":"2026-10-26T10:14:26Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":114,"customer_id":114,"first_transaction_id":1216,"last_transaction_id":1218,"txn_count":3,"total_amount":"14187/25","window_start":"2026-10-19T06:48:07Z","window_end":"2026-10-19T06:48:17Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":245,"customer_id":160,"first_transaction_id":117,"last_transaction_id":119,"txn_count":3,"total_amount":"2811/20","window_start":"2026-10-03T07:51:57Z","window_end":"2026-10-03T07:52:28Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":320,"customer_id":9,"first_transaction_id":839,"last_transaction_id":841,"txn_count":3,"total_amount":"36763/100","window_start":"2026-10-14T01:14:27Z","window_end":"2026-10-14T01:14:46Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":260,"customer_id":111,"first_transaction_id":1747,"last_transaction_id":1749,"txn_count":3,"total_amount":"4153/20","window_start":"2026-10-26T19:30:49Z","window_end":"2026-10-26T19:31:07Z","rule_name":"RULE_VELOCITY_60S"}
{"card_id":228,"customer_id":63,"first_transaction_id":882,"last_transaction_id":884,"txn_count":3,"total_amount":"1618/25","window_start":"2026-10-14T14:34:23Z","window_end":"2026-10-14T14:34:59Z","rule_name":"RULE_VELOCITY_60S"}
```

Transações de origem do alerta do cartão 100 (consulta real ao Postgres; as demais estão em `evidencias/04-alerta-fraude.txt`):

```
--- card_id=100 (transações 1726 a 1728)
 transaction_id | card_id | amount |  status  |         txn_ts         
----------------+---------+--------+----------+------------------------
           1726 |     100 | 193.20 | APPROVED | 2026-10-26 10:14:04+00
           1727 |     100 |  36.29 | APPROVED | 2026-10-26 10:14:23+00
           1728 |     100 | 235.61 | APPROVED | 2026-10-26 10:14:26+00
(3 rows)
```

As três transações ocorrem em 22 s, dentro da janela de 60 s.

**Controle negativo:** nenhum alerta para os cartões 126 ("quase": 2 transações) e 86 ("lento": 3 transações em 90 s), como esperado.

**Por que esta peça:** o Flink SQL permite declarar lógica de eventos complexa (`MATCH_RECOGNIZE` e temporal join) em SQL, sem microsserviços com estado em memória nem um banco de estado externo. O Flink gerencia o estado com *exactly-once* e tolerância a falhas.

## Camada 5 — Consumo e Operação (os quatro pilares)

### Consumo e menor privilégio (`desafio-consumer`)

Os alertas são lidos com a service account `desafio-consumer`, que tem só leitura no tópico `desafio.fraud.detected` e no Schema Registry. Scripts: `scripts/consume-alerts.sh` e `scripts/06-consume.sh`. Evidência completa: `evidencias/05-consumo.txt`.

**1. Leitura autorizada**

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

**2. Teste negativo de escrita**

```bash
$ echo '{"attempt": "unauthorized_write_test"}' | confluent kafka topic produce desafio.fraud.detected
Starting Kafka Producer. Use Ctrl-C or Ctrl-D to exit.
failed to produce offset -1: Broker: Topic authorization failed
```

Acesso negado pelas ACLs do Kafka: o consumidor não consegue escrever nem poluir dados.

### As quatro respostas operacionais

Evidência completa gerada por `scripts/07-operation.sh` em `evidencias/06-operacao.txt`.

#### ① Observabilidade

Medido na **Confluent Telemetry Metrics API** (`scripts/metrics.sh`), granularidade de 1 h, janela de 2026-11-16 a 2026-11-23:

- **Pico de ingestão (`received_bytes`):** `1.084.678` bytes (~1,08 MB) em 2026-11-17 13:00 UTC, na carga inicial (snapshot) do CDC.
- **Pico de egress (`sent_bytes`):** `7.019.047` bytes (~7,02 MB) em 2026-11-23 13:00 UTC, quando os statements Flink foram recriados e releram os tópicos desde o início.
- **Armazenamento retido (`retained_bytes`):** pico de `1.666.322` bytes (~1,67 MB).
- **Lag por grupo:** não medido. Os grupos desta entrega são efêmeros (cada leitura usa um `--group` novo e termina), então `confluent kafka consumer group list` devolve `None found`. Em produção o lag viria da métrica `consumer_lag_offsets` por `consumer_group_id`.

#### ② Segurança (matriz de menor privilégio)

| Identidade | Escopo | Permissões | Por quê |
|---|---|---|---|
| `desafio-producer` | Tópicos `desafio*`, `__debezium-heartbeat*` | `CREATE`, `WRITE`, `DESCRIBE` | O conector produz eventos e heartbeats |
| `desafio-producer` | Cluster Kafka | `DESCRIBE`, `DESCRIBE_CONFIGS` | Exigido pelo Debezium V2 |
| `desafio-producer` | Schema Registry (`Subject:desafio*`) | `DeveloperWrite`, `DeveloperRead`, `DataSteward` | Registrar schemas e aplicar tags `PII`/`PCI` |
| `desafio-consumer` | Tópicos `desafio*` | `READ`, `DESCRIBE` | Ler alertas e streams de negócio |
| `desafio-consumer` | Consumer groups `desafio-*` | `READ` | Entrar em grupos de consumo |
| `desafio-consumer` | Schema Registry (`Subject:desafio*`) | `DeveloperRead` | Ler Avro sem poder alterar schemas |
| `cdc_user` (Postgres) | Banco `neondb`, schema `public` | `LOGIN`, `REPLICATION`, `SELECT` | Ler o WAL e a publication sem DDL, escrita nem superuser |

#### ③ Custos e alavancas de otimização

Medido depois do teardown definitivo, com `confluent billing cost list --start-date 2026-11-01 --end-date 2026-11-24 -o json` (`scripts/cost.sh`), 68 linhas de 2026-11-16 a 2026-11-24, somadas por tipo (`evidencias/08-teardown-e-custo.txt`):

| Item | Consumo bruto | % do total |
|---|---|---|
| Flink (CFU-hora) — `desafio-pool` | US$ 97.91 | 47.4% |
| Conector CDC (task-hora) — `desafio-cdc` | US$ 80.00 | 38.8% |
| Cluster Kafka Basic (CKU-hora) — `desafio-basic` | US$ 28.35 | 13.7% |
| Rede (escrita) | US$ 0.07 | 0.0% |
| Rede (leitura) | US$ 0.05 | 0.0% |
| **Total bruto** | **US$ 206.38** | 100% |
| Crédito promocional `FREETRIAL400` | US$ -206.38 | |
| **Líquido cobrado no cartão** | **US$ 0.00** | |

O que gera custo é o tempo ligado, não o volume: a rede somou US$ 0.12 no total. Flink e conector respondem por cerca de 86% e cobram por hora enquanto existirem, com ou sem mensagens. Nos dias completos com tudo ligado (2026-11-19 a 2026-11-22), o ritmo ficou em torno de US$ 30,4 por dia.

**Duas alavancas, escolhidas pelos números acima:**
1. **Derrubar o que não está em uso (`./teardown.sh`).** Flink, conector e cluster não têm "pausa": só apagar interrompe a cobrança. O `./setup.sh` recria tudo em poucos minutos.
2. **Dimensionar o Flink ao mínimo e não manter statements sem consumidor.** O pool tem teto de 5 CFU (`--max-cfu 5`) e cada statement ativo consome CFU. Durante o desenvolvimento um 4º statement ficou rodando sem que nada lesse o resultado (foi removido). Manter só statements com consumidor, e o pool no menor teto que sustenta a carga, reduz a maior linha da fatura.

#### ④ Confiabilidade fim a fim e idempotência

| Trecho | Garantia | Mecanismo |
|---|---|---|
| **Postgres (WAL) → CDC Debezium** | *At-least-once* | Replication slot com LSN confirmado após o ACK do Kafka. Se o slot some no Postgres, o conector vai para `FAILED`; por isso `connectors/cdc.json` usa `snapshot.mode=when_needed`, que recria o slot e refaz o snapshot |
| **CDC → tópicos Kafka** | *At-least-once* | Tópicos com RF=3. A chave de cada mensagem é a PK da tabela (`transaction_id`, `account_id`, `customer_id`, `card_id`, `merchant_id`). Os tópicos do CDC têm 1 partição: a ordem por tópico é total |
| **Kafka → Flink SQL → tópico de alerta** | *Exactly-once* | Checkpoints periódicos de estado e transações Kafka no sink do Flink |
| **Tópico de alerta → consumidor** | *At-least-once* | **Elo fraco:** a entrega é garantida, mas o efeito externo (bloquear o cartão, notificar) exige **idempotência na aplicação consumidora**, chaveada por `(card_id, first_transaction_id, window_start)`, para evitar duplicação em rebalances e em snapshots refeitos |

## Como subir, parar e retomar

```bash
./setup.sh                    # sobe/retoma tudo, na ordem (idempotente: pode rodar de novo)
./scripts/status.sh           # o que existe agora (e portanto gera custo) — somente leitura
./teardown.sh --dry-run       # mostra o que seria apagado, sem apagar nada
./teardown.sh                 # apaga (pede para digitar "apagar")
./teardown.sh --yes           # apaga sem perguntar
./teardown.sh --keep-neon     # não mexe no slot de replicação do Postgres
```

- **Parar de gastar:** o cluster, o conector e o Flink cobram por hora enquanto existirem. O `./teardown.sh` apaga, nesta ordem: conectores → Flink (statements e compute pool) → role bindings do Schema Registry → API keys → cluster → service accounts → environment (leva o Schema Registry junto) → slot de replicação no Postgres. Ele só toca nos recursos com os nomes do `.env` e **nunca** no environment `default`.
- **Retomar:** `./setup.sh` recria tudo. Tópicos, schemas e dados do Kafka são recriados; o conector refaz o snapshot do Postgres. Os dados no Neon sobrevivem (o `setup.sh` só recarrega o seed se faltarem linhas; `./setup.sh --reseed` força).
- **O que o teardown não apaga:** o projeto Neon e seus dados, o `.env` e os arquivos de `evidencias/`. Para apagar o projeto Neon: `neon projects delete <id>` (o ID está em `.neon`).
- **Segredos:** as API keys ficam em `.state` (gitignored). O teardown apaga o `.state` junto com as keys, e o `setup.sh` gera keys novas.

## Custo total e teardown

**Custo declarado: US$ 206.38 de consumo bruto** (2026-11-16 a 2026-11-24), **totalmente coberto** pelo crédito promocional `FREETRIAL400` (US$ -206.38): **US$ 0.00 cobrados no cartão**. Consulta feita **depois** do `./teardown.sh` definitivo, com `./scripts/cost.sh 2026-11-01 2026-11-24`. Detalhe por item no pilar ③ e em `evidencias/08-teardown-e-custo.txt`.

O billing tem defasagem: a última linha diária devolvida é 2026-11-23, e o ambiente ficou ligado até ~00:35 UTC de 2026-11-24. Esse último trecho (cerca de meia hora) pode ainda não aparecer no total acima. Como o ambiente já não existe, nada novo é gerado.

### Verificação de listas vazias pós-teardown

Saída real do `./teardown.sh` definitivo (2026-11-24, `exit=0`), executado depois de `./teardown.sh --dry-run`. Ele só apagou recursos com os nomes do `.env`. Trecho completo, com os IDs de cada recurso, em `evidencias/08-teardown-e-custo.txt`:

```
== O que será apagado
   environment : env-n9oxv6 (desafio-final)
   cluster     : lkc-7ymrz1j (desafio-basic)
   Sch.Registry: lsrc-386v0kj (some junto com o environment)
   conectores  : lcc-xqwj5zq
   flink pools : lfcp-0x6qw75
   service acc.:  sa-wzp0wkg sa-5mn3dyg
   slot Neon   : slots 'desafio%' inativos
   .state local: será removido (as API keys morrem junto)

>> 1/8 conectores
   ok: confluent connect cluster delete lcc-xqwj5zq --cluster lkc-7ymrz1j --environment env-n9oxv6 --force
>> 2/8 Flink statements e compute pool
   (6 statements e o compute pool apagados)
   -- verificação (esperado: None found.)
   connect cluster list:
     None found.
   flink statement list:
     None found.
>> 3/8 role bindings do Schema Registry
>> 4/8 API keys
>> 5/8 cluster Kafka
>> 6/8 service accounts
>> 7/8 environment
>> 8/8 slot de replicação no Postgres
   drop slot desafio_cdc_slot
   slots 'desafio%' restantes: 0

== Verificação final (nada com o prefixo 'desafio-' deve restar)
-- confluent environment list
     Current |     ID     |  Name   | Stream Governance Package  
   ----------+------------+---------+----------------------------
             | env-2kpg72 | default | ESSENTIALS                 
-- confluent iam service-account list
   None found.
```

Conferência independente, com `./scripts/status.sh`, depois do teardown:

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

O projeto Neon e os 2000 registros continuam lá (o teardown não os apaga). O environment `default` da Confluent não foi tocado.

## Limites conhecidos

- O ciclo **teardown → setup do zero numa única execução, com `exit=0`**, não foi demonstrado depois das últimas correções dos scripts. O pipeline passou nos critérios com o `./setup.sh` corrigido (enriched 2000/2000/240, 6 alertas, conector e 3 statements `RUNNING`), e o vídeo foi gravado nesse ambiente.
- O custo tem defasagem de billing: o trecho final de 2026-11-24 pode não estar no total.
- `desafio.transactions.enriched` não tem consumidor: é saída analítica. A regra de fraude lê `desafio.public.transactions` direto.
- Os statements Flink rodam com o usuário que fez o login na CLI, não com uma service account dedicada.
- Mais detalhes (nomes, tópicos, schemas, quem liga em quem) em [`DOCUMENTACAO.md`](DOCUMENTACAO.md).
