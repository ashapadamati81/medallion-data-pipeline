"""
Sample e-commerce data generator.

Writes realistic-but-fake CSVs into ``data/raw/``, one file per table per
load date:

    customers_2026-09-30.csv / customers_2026-10-01.csv
    products_2026-09-30.csv  / products_2026-10-01.csv
    orders_2026-09-30.csv     / orders_2026-10-01.csv

The second batch deliberately contains:
  * ~5% new customers,
  * ~4% existing customers with changed attributes (address / tier / email)
    -> this is what the silver SCD Type 2 logic picks up,
  * a few new orders.

Intentional data-quality issues (for the pipeline to clean):
  * ~2% of customer emails are NULL or malformed,
  * duplicate order_ids across batches,
  * a few orders with quantity <= 0 or NULL keys,
  * inconsistent whitespace / casing in names and categories,
  * one malformed order_date.

Usage:
    python generate_sample_data.py [--seed 42] [--customers 500]
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

from config import LOAD_DATES, RAW_DIR

FIRST = ["Ava", "Liam", "Maya", "Noah", "Zoe", "Ethan", "Priya", "Lucas",
         "Sofia", "Mason", "Aisha", "James", "Nora", "Arjun", "Ella",
         "Daniel", "Ivy", "Owen", "Lena", "Kabir"]
LAST = ["Sharma", "Garcia", "Kim", "Patel", "Nguyen", "Smith", "Khan",
        "Lopez", "Chen", "Das", "Johnson", "Ali", "Brown", "Gupta",
        "Singh", "Lee", "Kumar", "Davis", "Rao", "Miller"]
CITIES = [("Austin", "TX", "78701"), ("Dallas", "TX", "75201"),
          ("Denver", "CO", "80202"), ("Seattle", "WA", "98101"),
          ("Chicago", "IL", "60601"), ("Miami", "FL", "33101"),
          ("Boston", "MA", "02108"), ("Phoenix", "AZ", "85001")]
STREETS = ["Maple Ave", "Oak St", "Cedar Ln", "Pine Rd", "Elm Dr",
           "Birch Way", "Willow Ct", "Aspen Blvd"]
TIERS = ["Standard", "Gold", "Platinum"]

PRODUCTS = [
    # (name, category, subcategory, base_price)
    ("Wireless Headphones X1", "Electronics", "Audio", 129.99),
    ("Bluetooth Speaker Mini", "Electronics", "Audio", 49.99),
    ("4K Monitor 27in", "Electronics", "Displays", 349.99),
    ("Mechanical Keyboard Pro", "Electronics", "Peripherals", 89.99),
    ("Ergonomic Office Chair", "Furniture", "Chairs", 249.99),
    ("Standing Desk 48in", "Furniture", "Desks", 399.99),
    ("LED Desk Lamp", "Furniture", "Lighting", 39.99),
    ("Organic Cotton T-Shirt", "Apparel", "Tops", 24.99),
    ("Denim Jacket Classic", "Apparel", "Outerwear", 79.99),
    ("Running Shoes AirFlex", "Apparel", "Footwear", 119.99),
    ("Stainless Water Bottle", "Home", "Kitchen", 29.99),
    ("Air Fryer XL", "Home", "Kitchen", 149.99),
    ("Robot Vacuum S9", "Home", "Cleaning", 299.99),
    ("Yoga Mat Pro", "Sports", "Fitness", 45.99),
    ("Dumbbell Set 25lb", "Sports", "Fitness", 89.99),
    ("Camping Tent 4P", "Sports", "Outdoor", 199.99),
]


def _messy(s: str, rng: random.Random, prob: float = 0.08) -> str:
    """Randomly add whitespace / casing noise to mimic source-system mess."""
    r = rng.random()
    if r < prob / 2:
        return "  " + s + " "
    if r < prob:
        return s.upper() if rng.random() < 0.5 else s.lower()
    return s


def gen_customers(n: int, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        city, state, z = rng.choice(CITIES)
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        r = rng.random()
        if r < 0.015:
            email = ""                      # NULL email
        elif r < 0.025:
            email = "not-an-email"          # malformed
        rows.append({
            "customer_id": f"C{i:05d}",
            "first_name": _messy(fn, rng),
            "last_name": _messy(ln, rng),
            "email": email.strip().lower() if email else "",
            "phone": f"{rng.randint(200, 989)}-{rng.randint(200, 989)}-"
                     f"{rng.randint(1000, 9999)}",
            "address": f"{rng.randint(100, 9899)} {rng.choice(STREETS)}",
            "city": city, "state": state, "zip": z,
            "signup_date": str(date(2023, 1, 1)
                               + timedelta(days=rng.randint(0, 900))),
            "customer_tier": rng.choices(TIERS, weights=[70, 22, 8])[0],
        })
    return rows


def mutate_customers(rows: list[dict], rng: random.Random):
    """Second-batch changes: new customers + attribute changes (SCD2)."""
    rows = [dict(r) for r in rows]
    # 5% brand-new customers
    start = len(rows) + 1
    for i in range(start, start + max(1, len(rows) // 20)):
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        city, state, z = rng.choice(CITIES)
        rows.append({
            "customer_id": f"C{i:05d}",
            "first_name": fn, "last_name": ln,
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
            "phone": f"{rng.randint(200, 989)}-{rng.randint(200, 989)}-"
                     f"{rng.randint(1000, 9999)}",
            "address": f"{rng.randint(100, 9899)} {rng.choice(STREETS)}",
            "city": city, "state": state, "zip": z,
            "signup_date": "2026-10-01",
            "customer_tier": "Standard",
        })
    # 4% attribute changes on existing customers
    for r in rng.sample(rows, max(1, len(rows) // 25)):
        if r["customer_id"] >= f"C{start:05d}":
            continue
        change = rng.choice(["tier", "address", "email"])
        if change == "tier":
            r["customer_tier"] = rng.choice(
                [t for t in TIERS if t != r["customer_tier"]])
        elif change == "address":
            city, state, z = rng.choice(CITIES)
            r["address"] = f"{rng.randint(100, 9899)} {rng.choice(STREETS)}"
            r["city"], r["state"], r["zip"] = city, state, z
        else:
            r["email"] = r["email"].replace("@example.com", "@example.org")
    return rows


def gen_products(rng: random.Random) -> list[dict]:
    return [{
        "product_id": f"P{i:04d}",
        "product_name": _messy(name, rng, 0.05),
        "category": cat.upper() if rng.random() < 0.1 else cat,  # casing noise
        "subcategory": sub,
        "unit_price": round(price * rng.uniform(0.97, 1.03), 2),
    } for i, (name, cat, sub, price) in enumerate(PRODUCTS, 1)]


def gen_orders(customers, products, load_date: str, n: int,
               rng: random.Random, dup_pool: list | None = None) -> list[dict]:
    rows = []
    ld = date.fromisoformat(load_date)
    for i in range(n):
        c, p = rng.choice(customers), rng.choice(products)
        qty = rng.choices([1, 2, 3, 4, 5], weights=[55, 25, 10, 6, 4])[0]
        r = rng.random()
        if r < 0.01:
            qty = -qty                       # invalid quantity
        # order_date == load_date: keeps every order inside the dimension's
        # known history window, so the gold as-was join never drops rows.
        # (In production, orders predating dim history need a backfill.)
        order_date = ld
        if r > 0.995:
            order_date_s = "not-a-date"      # malformed date
        else:
            order_date_s = str(order_date)
        rows.append({
            "order_id": f"O{load_date.replace('-', '')}-{i:05d}",
            "customer_id": c["customer_id"],
            "product_id": p["product_id"],
            "order_date": order_date_s,
            "quantity": qty,
            "unit_price": p["unit_price"],
            "discount_pct": round(rng.choice([0, 0, 0, 0.05, 0.1, 0.15]), 2),
        })
    if dup_pool:                             # duplicates across batches
        rows.extend(rng.sample(dup_pool, min(len(dup_pool), n // 50)))
    # a couple of NULL-key rows
    bad = dict(rng.choice(rows))
    bad["order_id"] = ""
    rows.append(bad)
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--customers", type=int, default=500,
                    help="customers in the first batch")
    ap.add_argument("--orders-per-day", type=int, default=3000)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    customers_b1 = gen_customers(args.customers, rng)
    products = gen_products(rng)
    write_csv(RAW_DIR / f"customers_{LOAD_DATES[0]}.csv", customers_b1)
    write_csv(RAW_DIR / f"products_{LOAD_DATES[0]}.csv", products)

    dup_pool: list | None = None
    for idx, ld in enumerate(LOAD_DATES):
        cust = customers_b1 if idx == 0 else mutate_customers(customers_b1,
                                                              rng)
        if idx == 1:
            write_csv(RAW_DIR / f"customers_{ld}.csv", cust)
            write_csv(RAW_DIR / f"products_{ld}.csv", products)
        orders = gen_orders(cust, products, ld, args.orders_per_day, rng,
                            dup_pool)
        write_csv(RAW_DIR / f"orders_{ld}.csv", orders)
        if idx == 0:
            dup_pool = rng.sample(orders, len(orders) // 40)
        print(f"[generate] {ld}: {len(cust)} customers, "
              f"{len(products)} products, {len(orders)} orders")


if __name__ == "__main__":
    main()
