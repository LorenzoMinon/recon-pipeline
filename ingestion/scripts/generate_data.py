"""
generate_data.py
Generates synthetic financial transaction data for ClearVault Corp.
Produces three CSVs (bank, ERP, billing) with deliberate discrepancies injected.
"""

import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

# ----- Configuration -----
NUM_CLEAN_TRANSACTIONS = 500
OUTPUT_DIR = "data/raw"

FACILITIES = [f"FAC-{str(i).zfill(3)}" for i in range(1, 21)]  # 20 facilities across US
PAYMENT_METHODS = ["credit_card", "ach", "cash", "check"]
STATUSES = ["completed", "pending", "failed"]
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 3, 31)


def random_date(start: datetime, end: datetime) -> str:
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def generate_base_transactions(n: int) -> list[dict]:
    """Generate n clean transactions that will be the source of truth."""
    transactions = []
    for i in range(n):
        transactions.append({
            "transaction_id": f"TXN-{str(i + 1).zfill(5)}",
            "customer_id": f"CUST-{str(random.randint(1, 200)).zfill(4)}",
            "facility_id": random.choice(FACILITIES),
            "amount": round(random.uniform(50, 500), 2),
            "transaction_date": random_date(START_DATE, END_DATE),
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choices(STATUSES, weights=[85, 10, 5])[0],  # mostly completed
        })
    return transactions


def build_bank_df(transactions: list[dict]) -> pd.DataFrame:
    """Bank extract — mostly clean, source of truth for amounts."""
    rows = []
    for t in transactions:
        rows.append({
            "transaction_id": t["transaction_id"],
            "customer_id": t["customer_id"],
            "facility_id": t["facility_id"],
            "amount": t["amount"],
            "transaction_date": t["transaction_date"],
            "payment_method": t["payment_method"],
            "status": t["status"],
            "source": "bank",
        })
    return pd.DataFrame(rows)


def build_erp_df(transactions: list[dict]) -> pd.DataFrame:
    """ERP system — has amount mismatches, date offsets, and missing transactions."""
    rows = []
    discrepancy_log = []

    for t in transactions:

        # Discrepancy type 1: missing transaction in ERP (~5% of records)
        if random.random() < 0.05:
            discrepancy_log.append({"transaction_id": t["transaction_id"], "type": "missing_in_erp"})
            continue  # skip — transaction won't appear in ERP

        amount = t["amount"]
        date = t["transaction_date"]
        status = t["status"]

        # Discrepancy type 2: amount mismatch (~4% of records)
        if random.random() < 0.04:
            amount = round(amount + random.uniform(-15, 15), 2)
            discrepancy_log.append({"transaction_id": t["transaction_id"], "type": "amount_mismatch"})

        # Discrepancy type 3: date offset (~3% of records)
        if random.random() < 0.03:
            original_date = datetime.strptime(date, "%Y-%m-%d")
            offset = random.choice([-2, -1, 1, 2])
            date = (original_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            discrepancy_log.append({"transaction_id": t["transaction_id"], "type": "date_mismatch"})

        # Discrepancy type 4: status inconsistency (~3% of records)
        if random.random() < 0.03 and status == "completed":
            status = "pending"
            discrepancy_log.append({"transaction_id": t["transaction_id"], "type": "status_mismatch"})

        rows.append({
            "transaction_id": t["transaction_id"],
            "customer_id": t["customer_id"],
            "facility_id": t["facility_id"],
            "amount": amount,
            "transaction_date": date,
            "payment_method": t["payment_method"],
            "status": status,
            "source": "erp",
        })

    return pd.DataFrame(rows), discrepancy_log


def build_billing_df(transactions: list[dict]) -> pd.DataFrame:
    """Billing system — has duplicate transactions and missing records."""
    rows = []
    discrepancy_log = []

    for t in transactions:

        # Discrepancy type 1: missing in billing (~4% of records)
        if random.random() < 0.04:
            discrepancy_log.append({"transaction_id": t["transaction_id"], "type": "missing_in_billing"})
            continue

        rows.append({
            "transaction_id": t["transaction_id"],
            "customer_id": t["customer_id"],
            "facility_id": t["facility_id"],
            "amount": t["amount"],
            "transaction_date": t["transaction_date"],
            "payment_method": t["payment_method"],
            "status": t["status"],
            "source": "billing",
        })

        # Discrepancy type 2: duplicate entry (~3% of records)
        if random.random() < 0.03:
            duplicate = rows[-1].copy()
            discrepancy_log.append({"transaction_id": t["transaction_id"], "type": "duplicate_in_billing"})
            rows.append(duplicate)

    return pd.DataFrame(rows), discrepancy_log


def main():
    print("Generating base transactions for ClearVault Corp...")
    transactions = generate_base_transactions(NUM_CLEAN_TRANSACTIONS)

    print("Building bank extract...")
    bank_df = build_bank_df(transactions)

    print("Building ERP extract...")
    erp_df, erp_discrepancies = build_erp_df(transactions)

    print("Building billing extract...")
    billing_df, billing_discrepancies = build_billing_df(transactions)

    # Save CSVs
    bank_df.to_csv(f"{OUTPUT_DIR}/bank_transactions.csv", index=False)
    erp_df.to_csv(f"{OUTPUT_DIR}/erp_transactions.csv", index=False)
    billing_df.to_csv(f"{OUTPUT_DIR}/billing_transactions.csv", index=False)

    # Print summary
    print("\n=== ClearVault Corp — Synthetic Data Summary ===")
    print(f"Bank:    {len(bank_df)} transactions")
    print(f"ERP:     {len(erp_df)} transactions")
    print(f"Billing: {len(billing_df)} transactions")
    print(f"\nDiscrepancies injected in ERP:     {len(erp_discrepancies)}")
    print(f"Discrepancies injected in Billing: {len(billing_discrepancies)}")
    print(f"\nFiles saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()