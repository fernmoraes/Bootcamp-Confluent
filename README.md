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
setup.sh          # (Sub-tarefa 8)
teardown.sh       # (Sub-tarefa 8)
db/               # DDL + gerador determinístico seed=42 + cdc_user
schemas/          # Avro (com.bootcamp.payments.*)
connectors/       # cdc.json
sql/              # statements Flink SQL
scripts/          # helpers: ACLs, test-compatibility, consumo, métricas, billing
evidencias/       # saídas capturadas
```

## Camada 1 — Fundação

Environment `desafio-final` + cluster Basic `desafio-basic` (aws/us-east-1).

Identidades: `desafio-producer` (WRITE em `desafio-*`), `desafio-consumer` (READ em `desafio-*` + grupo `desafio-*`); conta pessoal só em setup/teardown.

`<sua evidência aqui: confluent iam service-account list + acl list por SA>`

**Por que esta peça:** `<sua justificativa aqui>`

## Camada 2 — Contrato de dados

Schemas em `schemas/`, namespace `com.bootcamp.payments.*`, money como `bytes` + `decimal(15,2)`, tags de PII/PCI nos campos sensíveis.

Evolução BACKWARD demonstrada nos dois sentidos:

`<sua evidência aqui: test-compatibility aceitando campo novo com default>`

`<sua evidência aqui: test-compatibility REJEITANDO remoção de campo obrigatório>`

**Por que esta peça:** `<sua justificativa aqui>`

## Camada 3 — Ingestão (CDC)

Conector `PostgresCdcSourceV2` com `cdc_user` (menor privilégio) e `heartbeat.interval.ms=60000` — config em `connectors/cdc.json`, sem secrets (env vars).

Seed determinístico carregado (`seed=42`, gerador em `db/seed.py`, saída idêntica a cada execução — sha256 `5e292bef…b136`). Saída completa em `evidencias/00-seed-e-banco.txt`:

```
 customers | accounts | cards | merchants | transactions
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
```

Preparação do Postgres (`./db/apply.sh`): `wal_level=logical`, `REPLICA IDENTITY FULL` nas 5 tabelas, publication `desafio_pub`, e `cdc_user` com `LOGIN REPLICATION` e só `SELECT` (sem superuser, createdb, createrole nem escrita). O seed embute 6 cartões em rajada (3 a 5 transações em ≤ 40 s) que devem virar alerta, mais 2 casos-controle que NÃO devem (2 transações em 30 s; 3 transações em 90 s).

Os três tipos de evento capturados — envelopes completos em `evidencias/01-envelopes-cdc.json`:

- `op=c` (INSERT): `before=null`, `after` com a linha nova — `<sua evidência aqui>`
- `op=u` (UPDATE): `before` preenchido (`REPLICA IDENTITY FULL`) + `after` — `<sua evidência aqui>`
- `op=d` (DELETE): `after=null` + tombstone na sequência — `<sua evidência aqui>`

**Por que esta peça:** `<sua justificativa aqui>`

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

## Como subir e derrubar

```bash
./setup.sh      # sobe todas as camadas a partir do .env
./teardown.sh   # remove tudo e evita custo residual
```

(Scripts ainda não implementados — Sub-tarefa 8.)

## Custo total e teardown

Custo declarado: `<US$ X,XX — billing cost list>`

O comando de custo disponível na CLI, conforme `confluent billing cost list --help`, exige `--start-date` e `--end-date`:

```bash
confluent billing cost list --start-date <AAAA-MM-DD> --end-date <AAAA-MM-DD>
```

`./teardown.sh` executado ao final — verificação de listas vazias no output:

`<sua evidência aqui: connect cluster list / flink statement list → None found.>`
