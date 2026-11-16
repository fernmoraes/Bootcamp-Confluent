#!/usr/bin/env python3
"""Gerador determinístico de dados sintéticos (seed=42 por padrão).

Só biblioteca padrão. Escreve SQL em stdout (aplicado com psql por db/apply.sh)
e um resumo em stderr. Mesma seed => saída byte a byte idêntica.

Volumes (contrato do desafio):
    customers 200 | accounts 240 | cards 320 | merchants 60 | transactions 2000

Cenários de fraude embutidos (para a regra RULE_VELOCITY ser demonstrável):
    - 6 cartões "rajada": 3 a 5 transações em <= 40 s  -> DEVEM virar alerta
    - 1 cartão "quase":   2 transações em 30 s         -> NÃO alerta (só 2)
    - 1 cartão "lento":   3 transações a cada 45 s     -> NÃO alerta (3º cai em 90 s)
    Os cartões normais nunca têm duas transações a menos de 5 min uma da outra,
    então nenhum alerta acidental aparece nos dados de base.
"""
import argparse
import hashlib
import json
import random
import sys
from datetime import date, datetime, timedelta, timezone

N_CUSTOMERS, N_ACCOUNTS, N_CARDS, N_MERCHANTS, N_TXNS = 200, 240, 320, 60, 2000

# Janela fixa: não depende do relógio da máquina.
WINDOW_START = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_DAYS = 30
MIN_GAP_NORMAL_S = 300

FIRST = ["Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique",
         "Isabela", "João", "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Paulo",
         "Rafaela", "Samuel", "Tatiana", "Vinícius", "Beatriz", "Caio", "Débora", "Eduardo"]
LAST = ["Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira",
        "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes",
        "Barbosa", "Rocha", "Dias", "Nunes"]
CITIES = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre",
          "Salvador", "Recife", "Fortaleza", "Brasília", "Campinas"]
MCC = [("5411", "Supermercado"), ("5812", "Restaurante"), ("5541", "Posto de combustível"),
       ("5311", "Loja de departamento"), ("4121", "Transporte por app"), ("5732", "Eletrônicos"),
       ("5912", "Farmácia"), ("7832", "Cinema"), ("5651", "Vestuário"), ("4899", "Streaming")]
MERCHANT_WORDS = ["Central", "Express", "Prime", "Popular", "Bom Preço", "Nova Era", "Ponto",
                  "Mercadão", "Estrela", "Horizonte"]
BRANDS = ["VISA", "MASTERCARD", "ELO"]


def q(value):
    """Literal SQL seguro."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def ascii_slug(text):
    table = str.maketrans("áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
                          "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC")
    return text.translate(table).lower().replace(" ", ".")


def build(seed):
    rng = random.Random(seed)

    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)} {rng.choice(LAST)}"
        customers.append({
            "customer_id": cid,
            "full_name": name,
            "email": f"{ascii_slug(name)}.{cid}@exemplo.com.br",
            "phone": f"+55{rng.randint(11, 99)}9{rng.randint(10000000, 99999999)}",
            "birth_date": date(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 50)),
            "city": rng.choice(CITIES),
            "created_at": WINDOW_START - timedelta(days=rng.randint(365, 1800)),
        })

    accounts = []
    for aid in range(1, N_ACCOUNTS + 1):
        owner = aid if aid <= N_CUSTOMERS else rng.randint(1, N_CUSTOMERS)
        accounts.append({
            "account_id": aid,
            "customer_id": owner,
            "account_type": rng.choice(["CHECKING", "SAVINGS", "CREDIT"]),
            "status": rng.choices(["ACTIVE", "BLOCKED", "CLOSED"], [92, 5, 3])[0],
            "risk_tier": rng.choices(["LOW", "MEDIUM", "HIGH"], [70, 22, 8])[0],
            "balance": round(rng.uniform(100, 80000), 2),
            "currency": "BRL",
            "opened_at": WINDOW_START - timedelta(days=rng.randint(30, 1500)),
            "updated_at": WINDOW_START,
        })

    cards = []
    for kid in range(1, N_CARDS + 1):
        acc = kid if kid <= N_ACCOUNTS else rng.randint(1, N_ACCOUNTS)
        brand = rng.choice(BRANDS)
        cards.append({
            "card_id": kid,
            "account_id": acc,
            "card_number_mask": f"**** **** **** {rng.randint(0, 9999):04d}",
            "brand": brand,
            "status": rng.choices(["ACTIVE", "BLOCKED", "EXPIRED"], [90, 6, 4])[0],
            "expires_on": date(2027, 1, 1) + timedelta(days=rng.randint(0, 365 * 3)),
            "issued_at": WINDOW_START - timedelta(days=rng.randint(30, 1200)),
        })

    merchants = []
    for mid in range(1, N_MERCHANTS + 1):
        mcc, category = rng.choice(MCC)
        merchants.append({
            "merchant_id": mid,
            "name": f"{category.split()[0]} {rng.choice(MERCHANT_WORDS)} {mid:02d}",
            "mcc": mcc,
            "category": category,
            "city": rng.choice(CITIES),
            "country": "BR",
        })

    # ---- transações ----
    window_s = WINDOW_DAYS * 24 * 3600
    all_cards = [c["card_id"] for c in cards]
    rng.shuffle(all_cards)
    burst_cards = all_cards[:6]
    near_card, slow_card = all_cards[6], all_cards[7]
    special = set(burst_cards + [near_card, slow_card])
    normal_pool = [c for c in sorted(all_cards) if c not in special]
    card_account = {c["card_id"]: c["account_id"] for c in cards}

    txns = []

    def add(card_id, when):
        amount = min(max(round(rng.lognormvariate(4.0, 1.0), 2), 1.00), 5000.00)
        txns.append({
            "card_id": card_id,
            "account_id": card_account[card_id],
            "merchant_id": rng.randint(1, N_MERCHANTS),
            "amount": amount,
            "currency": "BRL",
            "status": rng.choices(["APPROVED", "DECLINED"], [97, 3])[0],
            "txn_ts": when,
        })

    fraud_truth = []
    for card_id, size in zip(burst_cards, [3, 3, 4, 3, 5, 3]):
        start = WINDOW_START + timedelta(seconds=rng.randint(86400, window_s - 86400))
        offsets = sorted(rng.sample(range(0, 40), size - 1))
        offsets = [0] + [o + 1 for o in offsets]
        for off in offsets:
            add(card_id, start + timedelta(seconds=off))
        fraud_truth.append({"card_id": card_id, "txns": size, "start": ts(start),
                            "expected": "ALERTA"})

    start = WINDOW_START + timedelta(seconds=rng.randint(86400, window_s - 86400))
    add(near_card, start)
    add(near_card, start + timedelta(seconds=30))
    fraud_truth.append({"card_id": near_card, "txns": 2, "start": ts(start),
                        "expected": "sem alerta (só 2 em 60 s)"})

    start = WINDOW_START + timedelta(seconds=rng.randint(86400, window_s - 86400))
    for k in range(3):
        add(slow_card, start + timedelta(seconds=45 * k))
    fraud_truth.append({"card_id": slow_card, "txns": 3, "start": ts(start),
                        "expected": "sem alerta (3 txns em 90 s, fora da janela de 60 s)"})

    seen = {}
    while len(txns) < N_TXNS:
        card_id = rng.choice(normal_pool)
        when = WINDOW_START + timedelta(seconds=rng.randint(0, window_s))
        if any(abs((when - t).total_seconds()) < MIN_GAP_NORMAL_S for t in seen.get(card_id, [])):
            continue
        seen.setdefault(card_id, []).append(when)
        add(card_id, when)

    txns.sort(key=lambda t: (t["txn_ts"], t["card_id"]))
    for i, t in enumerate(txns, start=1):
        t["transaction_id"] = i
    return customers, accounts, cards, merchants, txns, fraud_truth


def emit(table, rows, columns):
    lines = [f"INSERT INTO {table} ({', '.join(columns)}) VALUES"]
    body = []
    for r in rows:
        vals = []
        for c in columns:
            v = r[c]
            if isinstance(v, datetime):
                v = ts(v)
            elif isinstance(v, date):
                v = v.isoformat()
            vals.append(q(v))
        body.append("  (" + ", ".join(vals) + ")")
    lines.append(",\n".join(body) + ";")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    customers, accounts, cards, merchants, txns, truth = build(args.seed)

    parts = [
        f"-- seed={args.seed} — gerado por db/seed.py (determinístico)",
        "BEGIN;",
        "TRUNCATE transactions, cards, accounts, merchants, customers RESTART IDENTITY CASCADE;",
        emit("customers", customers, ["customer_id", "full_name", "email", "phone", "birth_date", "city", "created_at"]),
        emit("merchants", merchants, ["merchant_id", "name", "mcc", "category", "city", "country"]),
        emit("accounts", accounts, ["account_id", "customer_id", "account_type", "status", "risk_tier", "balance", "currency", "opened_at", "updated_at"]),
        emit("cards", cards, ["card_id", "account_id", "card_number_mask", "brand", "status", "expires_on", "issued_at"]),
        emit("transactions", txns, ["transaction_id", "card_id", "account_id", "merchant_id", "amount", "currency", "status", "txn_ts"]),
    ]
    for table, col, n in [("customers", "customer_id", len(customers)), ("accounts", "account_id", len(accounts)),
                          ("cards", "card_id", len(cards)), ("merchants", "merchant_id", len(merchants)),
                          ("transactions", "transaction_id", len(txns))]:
        parts.append(f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), {n});")
    parts.append("COMMIT;")
    sql = "\n".join(parts) + "\n"
    sys.stdout.write(sql)

    digest = hashlib.sha256(sql.encode()).hexdigest()
    sys.stderr.write(f"seed={args.seed} sha256={digest}\n")
    sys.stderr.write(f"linhas: customers={len(customers)} accounts={len(accounts)} cards={len(cards)} "
                     f"merchants={len(merchants)} transactions={len(txns)}\n")
    sys.stderr.write("cenarios de fraude (verdade esperada):\n")
    sys.stderr.write(json.dumps(truth, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
