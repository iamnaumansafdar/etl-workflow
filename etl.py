import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database connection
conn = psycopg2.connect(
    dbname="ecommerce", user="admin", password="password", host="postgres", port="5432"
)
cursor = conn.cursor()

def load_csv_to_db(file_path, table_name, columns, sort_column=None):
    logger.info(f"Loading {file_path} into {table_name}")
    df = pd.read_csv(file_path)

    # Clean data
    df = df.dropna(subset=columns)  # Drop rows with missing key fields

    # Convert parent_id to integer or None explicitly (for product_categories)
    if 'parent_id' in df.columns:
        df['parent_id'] = df['parent_id'].apply(lambda x: int(x) if pd.notna(x) else None)

    # Sort if a sort column is provided
    if sort_column and sort_column in df.columns:
        df = df.sort_values(by=sort_column)

    # Convert NumPy types to native Python types for all columns
    for col in df.columns:
        # print("Updating", col)
        df[col] = df[col].astype(str)
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(float if pd.api.types.is_float_dtype(df[col]) else int)

    # Special handling for product_categories (self-referential FK)
    if table_name == "product_categories":
        # Step 1: Insert main categories (parent_id = None)
        main_categories = df[df['parent_id'].isna()]
        main_tuples = [tuple(row) for row in main_categories[columns].values]
        if main_tuples:
            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
            logger.debug(f"Main categories to insert: {main_tuples[:5]}")  # Debug first 5 rows
            execute_values(cursor, query, main_tuples)
            conn.commit()
            logger.info(f"Loaded {len(main_tuples)} main categories into {table_name}")

        # Step 2: Insert subcategories (parent_id not null)
        sub_categories = df[df['parent_id'].notna()]
        sub_tuples = [tuple(row) for row in sub_categories[columns].values]
        if sub_tuples:
            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
            logger.debug(f"Subcategories to insert: {sub_tuples[:5]}")  # Debug first 5 rows
            execute_values(cursor, query, sub_tuples)
            conn.commit()
            logger.info(f"Loaded {len(sub_tuples)} subcategories into {table_name}")
    else:
        # Standard bulk insert for other tables
        data_tuples = [tuple(row) for row in df[columns].values]
        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cursor, query, data_tuples)
        conn.commit()
        logger.info(f"Loaded {len(data_tuples)} rows into {table_name}")

def run_etl():
    try:
        load_csv_to_db(
            "ecommerce_data/sample_product_categories.csv",
            "product_categories",
            ["category_id", "name", "description", "created_at"],
            sort_column="category_id"
        )
        load_csv_to_db(
            "ecommerce_data/sample_products.csv",
            "products",
            ["product_id", "name", "description", "price", "cost", "category_id", "sku", "inventory_count", "weight",
             "created_at", "is_active"]
        )
        load_csv_to_db(
            "ecommerce_data/sample_customers.csv",
            "customers",
            ["customer_id", "email", "first_name", "last_name", "street_address", "city", "state", "zip_code",
             "country", "phone", "registration_date", "last_login"]
        )
        load_csv_to_db(
            "ecommerce_data/sample_orders.csv",
            "orders",
            ["order_id", "customer_id", "order_date", "status", "payment_method", "shipping_address", "shipping_city",
             "shipping_state", "shipping_zip", "shipping_country", "processing_date", "shipping_date", "delivery_date",
             "total_amount"]
        )
        load_csv_to_db(
            "ecommerce_data/sample_order_items.csv",
            "order_items",
            ["order_item_id", "order_id", "product_id", "quantity", "price", "discount", "total"]
        )
        # Refresh materialized view after loading data
        logger.info("Refreshing materialized view product_sales_summary")
        cursor.execute("REFRESH MATERIALIZED VIEW product_sales_summary")
        conn.commit()
        logger.info("Materialized view product_sales_summary refreshed")
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_etl()