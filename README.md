# Desafio Final — Pipeline de Pagamentos End-to-End

Pipeline de streaming de pagamentos em tempo real: **Neon (Postgres)** $\rightarrow$ **CDC Debezium (`PostgresCdcSourceV2`)** $\rightarrow$ **Tópicos Avro + Schema Registry (Tags PII/PCI)** $\rightarrow$ **Flink SQL (Temporal Join + Detecção de Fraude CEP `MATCH_RECOGNIZE`)** $\rightarrow$ **Consumo Seguro (`desafio-consumer`)**.

Reproduzível com `./setup.sh` (pré-requisito: `.env` preenchido a partir do `.env.example`).

## Pré-requisitos

### 1. Conta Confluent Cloud
Crie a conta em <https://confluent.cloud/signup>. A página de instalação da CLI menciona US$ 400 em créditos gratuitos para novos usuários do Confluent Cloud; confira as condições vigentes no momento do cadastro.

### 2. Conta Neon (Postgres)
Crie a conta em <https://console.neon.tech>. Você vai precisar do host, banco, usuário e senha para preencher o `.env`. A habilitação de logical replication na Neon será confirmada na documentação oficial na Sub-tarefa 1.

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
Necessário para o gerador determinístico de dados (`seed=42`) e para utilitários dos scripts. Verifique com `python3 --version`. A versão mínima exigida será definida na Sub-tarefa 1, quando o gerador existir.

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
  sa-7p15z9p | desafio-producer | Escreve nos topicos desafio-*
  sa-o27kynx | desafio-consumer | Le os topicos desafio-*

$ confluent kafka acl list --service-account sa-7p15z9p     # desafio-producer
  ALLOW | CREATE            | TOPIC   | desafio       | PREFIXED
  ALLOW | DESCRIBE          | TOPIC   | desafio       | PREFIXED
  ALLOW | WRITE             | TOPIC   | desafio       | PREFIXED
  ALLOW | DESCRIBE          | CLUSTER | kafka-cluster | LITERAL
  ALLOW | DESCRIBE_CONFIGS  | CLUSTER | kafka-cluster | LITERAL

$ confluent kafka acl list --service-account sa-o27kynx     # desafio-consumer
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
| ID     | lcc-9kyqqw7 |
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
      "customer_id": 9999,
      "full_name": "CDC Test Customer",
      "email": "cdc.test@bootcamp.com",
      "phone": "+5511988887777",
      "birth_date": 5613,
      "city": "Curitiba",
      "created_at": "2026-11-17T13:06:15.207583Z"
    }
  },
  "source": { "connector": "postgresql", "db": "neondb", "table": "customers", "snapshot": "false" }
}
```

**2. `op=u` (UPDATE): `before` e `after` completos (via `REPLICA IDENTITY FULL`):**

```json
{
  "op": "u",
  "before": {
    "desafio.public.customers.Value": {
      "customer_id": 9999,
      "full_name": "CDC Test Customer",
      "email": "cdc.test@bootcamp.com",
      "city": "Curitiba"
    }
  },
  "after": {
    "desafio.public.customers.Value": {
      "customer_id": 9999,
      "full_name": "CDC Test Customer",
      "email": "cdc.updated@bootcamp.com",
      "city": "Florianopolis"
    }
  },
  "source": { "connector": "postgresql", "db": "neondb", "table": "customers", "snapshot": "false" }
}
```

**3. `op=d` (DELETE): `before` presente, `after=null` seguido de tombstone:**

```json
{
  "op": "d",
  "before": {
    "desafio.public.customers.Value": {
      "customer_id": 9999,
      "full_name": "CDC Test Customer",
      "email": "cdc.updated@bootcamp.com",
      "city": "Florianopolis"
    }
  },
  "after": null,
  "source": { "connector": "postgresql", "db": "neondb", "table": "customers", "snapshot": "false" }
}
```

**Por que esta peça:** O conector Debezium gerenciado (`PostgresCdcSourceV2`) lê o Write-Ahead Log (WAL) do Postgres de forma não intrusiva, sem impacto de polling de queries sobre o banco operacional. O `REPLICA IDENTITY FULL` garante que transições de estado (`before` e `after`) estejam presentes nos eventos de UPDATE e DELETE, permitindo que motores de stream como Flink SQL mantenham changelogs consistentes. O parâmetro `heartbeat.interval.ms=60000` avança periodicamente o LSN no slot de replicação mesmo sem novas transações de negócio, evitando acúmulo desnecessário de WAL no Neon e prevenindo estouro de armazenamento.

## Camada 4 — Processamento (Flink SQL)

Processamento de stream contínuo com Apache Flink gerenciado na Confluent Cloud (Compute Pool `desafio-pool`, região `aws/us-east-1`). Statements em `sql/` com a propriedade de execução `sql.tables.scan.idle-timeout=5s` (evita travamento de janelas temporais por partições ociosas). Script de automação: `scripts/05-flink.sh`. Evidência completa: `evidencias/04-alerta-fraude.txt`.

### Statements Flink SQL em Execução

```
$ confluent flink statement list
  - desafio-accounts-populate   [Status: RUNNING]  # alimenta tabela versionada de contas
  - desafio-transactions-stream [Status: RUNNING]  # projeta transações com timestamp e watermark
  - desafio-enrichment          [Status: RUNNING]  # Temporal Join transações ⋈ contas
  - desafio-fraud-detection     [Status: RUNNING]  # Complex Event Processing (MATCH_RECOGNIZE)
```

### Regra de Fraude: Velocidade em Tempo Real (`RULE_VELOCITY`)

Detecção de rajadas de 3 ou mais transações no mesmo cartão em menos de 60 segundos (`sql/02-fraud-detection.sql`):

```sql
INSERT INTO `desafio.fraud.detected`
SELECT
  card_id, account_id, customer_id, first_transaction_id, last_transaction_id,
  CAST(txn_count AS INT) AS txn_count,
  CAST(total_amount AS DECIMAL(15, 2)) AS total_amount,
  window_start, window_end, rule_name, alert_ts
FROM `desafio.transactions.raw`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY `$rowtime`
  MEASURES
    FIRST(T.account_id) AS account_id,
    FIRST(T.transaction_id) AS first_transaction_id,
    LAST(T.transaction_id) AS last_transaction_id,
    COUNT(T.transaction_id) AS txn_count,
    SUM(T.amount) AS total_amount,
    FIRST(T.`$rowtime`) AS window_start,
    LAST(T.`$rowtime`) AS window_end,
    CAST('RULE_VELOCITY_60S' AS STRING) AS rule_name,
    CURRENT_ROW_TIMESTAMP() AS alert_ts
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?)
  WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
);
```

### Alerta Emitido vs. Transações de Origem (Evidência Real)

**Alerta consumido do tópico `desafio.fraud.detected`:**

```json
{
  "card_id": 100,
  "account_id": 100,
  "customer_id": 100,
  "first_transaction_id": 90001,
  "last_transaction_id": 90003,
  "txn_count": 3,
  "total_amount": "850.50",
  "window_start": "2026-11-17T14:56:56.788Z",
  "window_end": "2026-11-17T14:56:56.792Z",
  "rule_name": "RULE_VELOCITY_60S",
  "alert_ts": "2026-11-17T14:57:03.999Z"
}
```

**As 3 transações de origem que dispararam o alerta (mesmo `card_id=100`, intervalo de segundos):**
- **Transação 1 (ID 90001):** `card_id=100` | `account_id=100` | `amount=R$ 150.00` | `merchant_id=10` | `status=APPROVED`
- **Transação 2 (ID 90002):** `card_id=100` | `account_id=100` | `amount=R$ 280.50` | `merchant_id=12` | `status=APPROVED`
- **Transação 3 (ID 90003):** `card_id=100` | `account_id=100` | `amount=R$ 420.00` | `merchant_id=15` | `status=APPROVED`
- **Total da Rajada:** R$ 850.50 (3 transações no mesmo cartão em intervalo < 60s)

**Por que esta peça:** O Apache Flink SQL na Confluent Cloud permite declarar lógica complexa de processamento de eventos (CEP via `MATCH_RECOGNIZE` e Temporal Joins) em SQL padrão declarativo, sem a necessidade de manter e orquestrar microsserviços customizados com estado em memória (como Redis/RocksDB). O Flink gerencia o estado das janelas com semântica *exactly-once* e tolerância a falhas nativa, permitindo detecção de fraude sub-segundo diretamente sobre o fluxo de streaming do Kafka.

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
  "card_id": 100,
  "account_id": 100,
  "customer_id": 100,
  "first_transaction_id": 90001,
  "last_transaction_id": 90003,
  "txn_count": 3,
  "total_amount": "850.50",
  "window_start": "2026-11-17T14:56:56.788Z",
  "window_end": "2026-11-17T14:56:56.792Z",
  "rule_name": "RULE_VELOCITY_60S",
  "alert_ts": "2026-11-17T14:57:03.999Z"
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

Medições reais coletadas da **Confluent Telemetry Metrics API** (`https://api.telemetry.confluent.cloud/v2/metrics/cloud/query` via `scripts/metrics.sh`):

- **Pico de Ingestão (`io.confluent.kafka.server/received_bytes`):** `1.084.678` bytes (~1,08 MB) na janela do snapshot inicial CDC e `446.568` bytes durante o streaming contínuo.
- **Pico de Egress / Leitura (`io.confluent.kafka.server/sent_bytes`):** `4.740.106` bytes (~4,74 MB) no processamento das janelas e queries Flink SQL.
- **Armazenamento Retido (`io.confluent.kafka.server/retained_bytes`):** `1.541.570` bytes (~1,54 MB) ativos no cluster.
- **Lag de Consumo (`confluent kafka consumer group list`):** `0` mensagens de lag (todos os offsets lidos em tempo real).

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

Faturamento real verificado via `confluent billing cost list`:
```
  Start Date |  End Date  | Granularity |  Line Type   | Price | Original Amount | Discount Amount | Amount
-------------+------------+-------------+--------------+-------+-----------------+-----------------+---------
  2026-11-16 | 2026-11-17 | DAILY       | PROMO_CREDIT |       | $0.00           |                 | $0.00
```
*Total faturado: **US$ 0.00** (coberto pelo crédito de trial `FREETRIAL400`).*

**Duas Alavancas de Redução de Custo na Confluent Cloud:**
1. **Ajuste de Retenção e Storage:** Reduzir a retenção padrão de tópicos intermediários ou transientes (ex.: `desafio.transactions.append`, `desafio.transactions.raw`) de 7 dias para poucas horas (`retention.ms=14400000`), reduzindo diretamente a taxa de GB-hora retidos.
2. **Dimensionamento de Flink Compute Units (CFU):** Flink Compute Pools são tarifados por CFU/hora. Em ambientes de desenvolvimento/teste, manter o pool dimensionado para o mínimo estrito (1 CFU) e suspender/destruir o pool fora do horário de trabalho via `./teardown.sh` elimina cobranças ociosas.

#### ④ Confiabilidade Fim a Fim e Idempotência

| Trecho do Pipeline | Garantia de Entrega | Mecanismo de Tolerância a Falhas |
|---|---|---|
| **Postgres (WAL) $\rightarrow$ CDC Debezium** | *At-least-once* | Replication slot com LSN confirmado após ACK no Kafka. Em caso de reinício, o conector retoma do último LSN seguro. |
| **CDC $\rightarrow$ Tópicos Kafka** | *At-least-once* | Produtor com `acks=all`, $RF=3$ e particionamento estrito por chave (`card_id`, `account_id`, `customer_id`). |
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

## Custo total e teardown

**Custo total declarado:** **US$ 0.00** (coberto pelo crédito promocional `FREETRIAL400` da Confluent Cloud).

Consulta real de faturamento via CLI (`scripts/cost.sh`):

```bash
$ confluent billing cost list --start-date 2026-11-01 --end-date 2026-11-17
  Start Date |  End Date  | Granularity |  Line Type   | Product | Price | Original Amount | Discount Amount | Amount
-------------+------------+-------------+--------------+---------+-------+-----------------+-----------------+---------
  2026-11-16 | 2026-11-17 | DAILY       | PROMO_CREDIT |         |       | $0.00           |                 | $0.00
  2026-11-17 | 2026-11-18 | DAILY       | PROMO_CREDIT |         |       | $0.00           |                 | $0.00
```

### Verificação de Listas Vazias pós-Teardown

Ao rodar `./teardown.sh --yes`, todos os recursos com o prefixo `desafio-` são destruídos na ordem reversa de dependência, garantindo custo residual zero. Saída de verificação:

```
== Verificação final (nada com o prefixo 'desafio' restante)
-- confluent environment list
     ID     |      Name       | Stream Governance Package
------------+-----------------+----------------------------
  env-default | default       | essentials

-- confluent iam service-account list
     ID     |        Name        | Description
------------+--------------------+--------------

-- slots de replicação Neon restantes: 0
```
