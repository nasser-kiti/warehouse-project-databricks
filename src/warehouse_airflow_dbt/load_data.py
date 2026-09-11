import argparse
import logging
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Order matters: tables must be loaded after the tables they reference
# (employees -> stores; orders -> customers, stores; order_items -> orders, products).
TABLES: dict[str, tuple[str, ...]] = {
    "customers": (
        "customer_id", "first_name", "last_name", "email", "phone", "city",
        "province", "country", "created_timestamp", "updated_timestamp", "is_active",
    ),
    "stores": (
        "store_id", "store_name", "city", "province", "country",
        "created_timestamp", "updated_timestamp", "is_active",
    ),
    "products": (
        "product_id", "product_name", "category", "brand", "price",
        "created_timestamp", "updated_timestamp", "is_active",
    ),
    "employees": (
        "employee_id", "store_id", "first_name", "last_name", "email",
        "job_title", "salary", "created_timestamp", "updated_timestamp", "is_active",
    ),
    "orders": (
        "order_id", "customer_id", "store_id", "order_timestamp", "payment_method",
        "order_status", "total_amount", "created_timestamp", "updated_timestamp", "is_active",
    ),
    "order_items": (
        "order_item_id", "order_id", "product_id", "quantity", "unit_price",
        "line_amount", "created_timestamp", "updated_timestamp", "is_active",
    ),
}


def get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")
    return database_url


def row_count(cursor, table_name: str) -> int:
    cursor.execute(
        sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name))
    )
    return cursor.fetchone()[0]


def assert_tables_empty(cursor) -> None:
    populated = {
        table_name: count
        for table_name in TABLES
        if (count := row_count(cursor, table_name))
    }
    if populated:
        raise RuntimeError(
            "Refusing to load into populated tables: "
            + ", ".join(f"{table} ({count} rows)" for table, count in populated.items())
        )


def load_table(cursor, data_directory: Path, table_name: str, columns: tuple[str, ...]) -> int:
    csv_path = data_directory / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing data file for '{table_name}': {csv_path}")

    copy_sql = sql.SQL("COPY {table} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)").format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )
    with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        cursor.copy_expert(copy_sql, csv_file)

    return row_count(cursor, table_name)


def load_data(database_url: str, data_directory: Path) -> dict[str, int]:
    loaded_rows: dict[str, int] = {}
    connection = psycopg2.connect(database_url)
    try:
        with connection:
            with connection.cursor() as cursor:
                assert_tables_empty(cursor)
                for table_name, columns in TABLES.items():
                    loaded_rows[table_name] = load_table(cursor, data_directory, table_name, columns)
    finally:
        connection.close()
    return loaded_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load seed CSV data into Postgres.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "assets" / "data",
        help="Directory containing one CSV file per table (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database_url = get_database_url()
    try:
        loaded_rows = load_data(database_url, args.data_dir)
    except (RuntimeError, FileNotFoundError, psycopg2.Error) as exc:
        logger.error(exc)
        raise SystemExit(1) from exc

    for table_name, count in loaded_rows.items():
        logger.info("%s: %d rows loaded", table_name, count)


if __name__ == "__main__":
    main()
