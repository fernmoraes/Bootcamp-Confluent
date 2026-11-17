# Desafio Final — Pipeline de pagamentos

> **Rascunho (Sub-tarefa 0).** Os trechos `<sua evidência aqui>` serão preenchidos com a saída real da execução. Nada abaixo afirma resultado de execução ainda.

Pipeline: Neon (Postgres) → CDC Debezium (`PostgresCdcSourceV2`) → tópicos Avro → Flink SQL (enriquecimento + fraude) → consumo.

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

Statements em `sql/`: temporal join transações ⋈ contas (PK nos dois lados, `sql.tables.scan.idle-timeout=5s`) e `RULE_VELOCITY` via `MATCH_RECOGNIZE` (`PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND`, `PARTITION BY card_id`).

`<sua evidência aqui: o alerta emitido no tópico desafio.fraud.detected + as 3 transações (mesmo card_id, <60s) que o dispararam>`

**Por que esta peça:** `<sua justificativa aqui>`

## Camada 5 — Operação: as quatro respostas

| Pilar | Resposta (medida) |
|---|---|
| ① Observabilidade | `<pico de received_bytes e lag por grupo — curl da Metrics API>` |
| ② Segurança | `<tabela: SA → ACLs → porquê>` |
| ③ Custos | `<billing cost list do período + 2 alavancas>` |
| ④ Confiabilidade | `<tabela trecho a trecho; elo fraco: consumer → efeito (idempotência é código meu)>` |

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

Custo declarado: `<US$ X,XX — billing cost list>`

O comando de custo disponível na CLI, conforme `confluent billing cost list --help`, exige `--start-date` e `--end-date`:

```bash
confluent billing cost list --start-date <AAAA-MM-DD> --end-date <AAAA-MM-DD>
```

`./teardown.sh` executado ao final — verificação de listas vazias no output:

`<sua evidência aqui: connect cluster list / flink statement list → None found.>`
