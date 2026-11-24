# Desafio Final — Pipeline de Pagamentos End-to-End

**Neon (Postgres)** → **CDC Debezium (`PostgresCdcSourceV2`)** → **tópicos Avro + Schema Registry (tags PII/PCI)** → **Flink SQL (enriquecimento + detecção de fraude)** → **consumo com menor privilégio**.

Reproduzível com `./setup.sh` (pré-requisito: `.env` preenchido a partir do `.env.example`).

## Camada 1 — Fundação

Environment `desafio-final` e cluster Basic `desafio-basic` (aws/us-east-1), com duas service accounts: `desafio-producer` (escreve em `desafio*`) e `desafio-consumer` (lê `desafio*`). A conta pessoal é usada no setup/teardown e para submeter os statements Flink; as aplicações usam as service accounts. Script: `scripts/01-foundation.sh`. Evidência: `evidencias/02-fundacao.txt`.

```
$ confluent iam service-account list
      ID     |       Name       |          Description
  sa-0jm6vv2 | desafio-producer | Escreve nos topicos desafio*
  sa-mkxgqqq | desafio-consumer | Le os topicos desafio*

$ confluent kafka acl list --service-account sa-0jm6vv2     # desafio-producer
  ALLOW | CREATE            | TOPIC   | __debezium-heartbeat | PREFIXED
  ALLOW | CREATE            | TOPIC   | desafio              | PREFIXED
  ALLOW | DESCRIBE          | CLUSTER | kafka-cluster        | LITERAL
  ALLOW | DESCRIBE          | TOPIC   | __debezium-heartbeat | PREFIXED
  ALLOW | DESCRIBE          | TOPIC   | desafio              | PREFIXED
  ALLOW | DESCRIBE_CONFIGS  | CLUSTER | kafka-cluster        | LITERAL
  ALLOW | WRITE             | TOPIC   | __debezium-heartbeat | PREFIXED
  ALLOW | WRITE             | TOPIC   | desafio              | PREFIXED

$ confluent kafka acl list --service-account sa-mkxgqqq     # desafio-consumer
  ALLOW | DESCRIBE | TOPIC | desafio  | PREFIXED
  ALLOW | READ     | GROUP | desafio- | PREFIXED
  ALLOW | READ     | TOPIC | desafio  | PREFIXED
```

**Por que esta peça:** duas identidades com ACLs por prefixo aplicam o menor privilégio: a credencial de uma aplicação não alcança nada fora de `desafio*`.

## Camada 2 — Contrato de dados

Schemas Avro em `schemas/`, namespace `com.bootcamp.payments.*`. Valores monetários são `bytes` com `logicalType: decimal(15,2)`. As tags `PII` (`customers.full_name`, `email`, `phone`, `birth_date`) e `PCI` (`cards.card_number_mask`) estão no catálogo do Stream Governance. A compatibilidade global do Schema Registry é `BACKWARD`. Script: `scripts/03-schemas.sh`.

Evidência da execução de 17/11 (`evidencias/03-contrato-de-dados.txt`), com o modo de cada cenário como foi testado:

```
# Cenário 1 — BACKWARD: campo novo com default → ACEITO
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-compatible.avsc --type avro --subject desafio.test.evolution-value --version latest
| Compatible | true |

# Cenário 2 — BACKWARD: campo obrigatório sem default → REJEITADO
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-add-required.avsc --type avro --subject desafio.test.evolution-value --version latest
| Compatible | false |

# Cenário 3 — FULL: remoção de campo obrigatório → REJEITADO
$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-incompatible.avsc --type avro --subject desafio.test.evolution-full-value --version latest
| Compatible | false |
```

**Por que esta peça:** o contrato Avro no Schema Registry impede que uma mudança quebre produtores e consumidores sem aviso.

## Camada 3 — Ingestão (CDC)

Conector gerenciado `PostgresCdcSourceV2` (`connectors/cdc.json`, sem secrets: só variáveis de ambiente). Usa a API key do `desafio-producer`, conecta com o `cdc_user` (`LOGIN REPLICATION` e só `SELECT`), publication `desafio_pub`, `snapshot.mode=when_needed` e `heartbeat.interval.ms=60000`. Script: `scripts/04-connector.sh`.

Seed determinístico (`db/seed.py`, `seed=42`). Evidência: `evidencias/00-seed-e-banco.txt`.

```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
```

Os três tipos de evento, ciclo `INSERT` → `UPDATE` → `DELETE` (envelopes completos em `evidencias/01-envelopes-cdc.json`):

- `op=c` (INSERT): `before=null` e `after` com a linha nova.
- `op=u` (UPDATE): `before` e `after` completos, por causa do `REPLICA IDENTITY FULL`.
- `op=d` (DELETE): `before` presente e `after=null`, seguido de um tombstone (mesma chave, valor nulo).

**Por que esta peça:** o Debezium lê o WAL sem ficar consultando as tabelas, então não pesa no banco operacional.

## Camada 4 — Processamento (Flink SQL)

Compute pool `desafio-pool` (`--max-cfu 5`). Statements em `sql/`, executados com `sql.tables.scan.idle-timeout=5s`. A tabela do CDC é ajustada com `changelog.mode='append'` e `txn_time` (de `txn_ts`) como tempo de evento, com watermark de 5 s. O `desafio-enrichment` faz um temporal join transações ⋈ contas (`FOR SYSTEM_TIME AS OF t.txn_time`), com PK `account_id` no lado versionado (`desafio.accounts.versioned`). Script: `scripts/05-flink.sh`. Evidência: `evidencias/04-alerta-fraude.txt`.

```
$ confluent flink statement list
  - desafio-accounts-populate [Status: RUNNING]
  - desafio-enrichment [Status: RUNNING]
  - desafio-fraud-detection [Status: RUNNING]
```

A regra `RULE_VELOCITY` (`MATCH_RECOGNIZE`, `PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND`, `PARTITION BY card_id`) emitiu 6 alertas, um por rajada do seed, e nenhum para os cartões-controle 126 e 86. Um deles, o do cartão 100:

```json
{"card_id":100,"customer_id":100,"first_transaction_id":1726,"last_transaction_id":1728,"txn_count":3,"total_amount":"4651/10","window_start":"2026-10-26T10:14:04Z","window_end":"2026-10-26T10:14:26Z","rule_name":"RULE_VELOCITY_60S"}
```

(total_amount 4651/10 = 465,10: é como a CLI mostra decimal Avro.)

As 3 transações que o dispararam (consulta real ao Postgres), 22 s entre a primeira e a última:

```
 transaction_id | card_id | amount |  status  |         txn_ts
----------------+---------+--------+----------+------------------------
           1726 |     100 | 193.20 | APPROVED | 2026-10-26 10:14:04+00
           1727 |     100 |  36.29 | APPROVED | 2026-10-26 10:14:23+00
           1728 |     100 | 235.61 | APPROVED | 2026-10-26 10:14:26+00
```

Decisões de projeto:

- O `05-flink.sh` espera as 240 contas na tabela versionada antes de subir o join, porque todas as contas têm a mesma data de atualização e o join só avança pelo `idle-timeout`.
- A regra de fraude roda sobre as transações e só depois busca a conta, porque o temporal join grava fora da ordem de tempo de evento (`evidencias/07-ordem-enriched.txt`).

**Por que esta peça:** o Flink SQL expressa o padrão de fraude e o join em SQL, sem serviço com estado próprio.

## Camada 5 — Operação

Evidência: `evidencias/06-operacao.txt` (gerada por `scripts/07-operation.sh`) e `evidencias/05-consumo.txt`.

| Pilar | Resultado |
|---|---|
| **① Observabilidade** | Pico de `received_bytes`: 542.172 bytes (2026-11-23 20:00 UTC, Metrics API, granularidade de 1 h). Lag não medido: os grupos desta entrega são efêmeros (cada leitura usa um `--group` novo e termina). Em produção, `consumer_lag_offsets` por grupo |
| **② Segurança** | Ver tabela abaixo. O `desafio-consumer` lê `desafio.fraud.detected`, mas escrever nele devolve `Topic authorization failed` (`evidencias/05-consumo.txt`) |
| **③ Custo** | Custo de um dia com o pipeline ligado: **US$ 30,37** (billing de 22/11), coberto pelo crédito de teste; líquido US$ 0,00. Duas alavancas: derrubar com `./teardown.sh` o que não está em uso (Flink, conector e cluster cobram por hora e não têm pausa) e manter só statements com consumidor, com o pool no menor teto que sustenta a carga |
| **④ Confiabilidade** | Ver tabela abaixo. O elo fraco é o consumidor: o efeito externo (bloquear o cartão, notificar) exige idempotência na aplicação, chaveada por `(card_id, first_transaction_id, window_start)` |

Segurança (SA → ACLs → porquê):

| Identidade | ACLs | Por quê |
|---|---|---|
| `desafio-producer` | `CREATE`, `WRITE`, `DESCRIBE` em `desafio*` e `__debezium-heartbeat*`; `DESCRIBE`, `DESCRIBE_CONFIGS` no cluster | O conector produz eventos e heartbeats |
| `desafio-consumer` | `READ`, `DESCRIBE` em `desafio*`; `READ` em grupos `desafio-*` | Lê alertas sem poder escrever |
| `cdc_user` (Postgres) | `LOGIN`, `REPLICATION`, `SELECT` | Lê o WAL sem DDL nem escrita |

Confiabilidade trecho a trecho:

| Trecho | Garantia |
|---|---|
| Postgres → Debezium | *At-least-once* (LSN do replication slot) |
| Debezium → Kafka | *At-least-once* |
| Kafka → Flink SQL → tópico de alerta | *Exactly-once* (checkpoints e transações) |
| Tópico de alerta → consumidor | *At-least-once*; **elo fraco**, exige idempotência |

## Custo e teardown

**Custo de um dia com o pipeline ligado: US$ 30,37 (billing de 22/11), coberto pelo crédito de teste; líquido US$ 0,00.** Consulta: `./scripts/cost.sh 2026-11-22 2026-11-23`. Saída completa em `evidencias/08-teardown-e-custo.txt`.

Cluster, conector e Flink cobram por hora enquanto existirem. Ao terminar, rode `./teardown.sh --dry-run` e depois `./teardown.sh`. Verificação de listas vazias do último teardown (`evidencias/08-teardown-e-custo.txt`):

```
   -- verificação (esperado: None found.)
   connect cluster list:
     None found.
   flink statement list:
     None found.
```

## Como rodar

Pré-requisitos:

- Conta Confluent Cloud
- Conta Neon (com logical replication habilitada no projeto)
- Confluent CLI e `jq`
- `psql`
- Python 3

```bash
cp .env.example .env          # preencha o .env (ele está no .gitignore)
./setup.sh                    # sobe tudo, na ordem (idempotente)
./scripts/status.sh           # o que existe agora na nuvem (somente leitura)
./teardown.sh --dry-run       # mostra o que seria apagado
./teardown.sh                 # apaga (pede para digitar "apagar")
```

Se um passo falhar logo depois de criar recursos (propagação na nuvem), rode `./setup.sh` de novo: ele é idempotente. Detalhes de nomes, tópicos e schemas em [`DOCUMENTACAO.md`](DOCUMENTACAO.md).
