import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import time
import pytest
from flytekit import task, workflow, Resources
from typing import List, Optional, Dict
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Database connection using environment variables
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DATABASE_NAME", "ecommerce"),
        user=os.getenv("DATABASE_USER", "admin"),
        password=os.getenv("DATABASE_PASSWORD", "password"),
        host=os.getenv("DATABASE_HOST", "postgres"),
        port=os.getenv("DATABASE_PORT", "5432")
    )


# Flyte task to extract data from CSV with chunking
@task(requests=Resources(cpu="1", mem="1Gi"))
def extract_csv(file_path: str, chunk_size: int = 10000) -> List[pd.DataFrame]:
    logger.info(f"Extracting data from {file_path} in chunks of {chunk_size}")
    chunks = []
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunks.append(chunk)
    return chunks


# Flyte task to concatenate chunks into a single DataFrame
@task(requests=Resources(cpu="1", mem="2Gi"))
def concatenate_chunks(chunks: List[pd.DataFrame]) -> pd.DataFrame:
    logger.info("Concatenating chunks into a single DataFrame")
    return pd.concat(chunks)


# Flyte task to generate and load time dimension data
@task(requests=Resources(cpu="1", mem="1Gi"))
def populate_dim_time(start_date: str = "2021-01-01", end_date: str = "2025-12-31") -> int:
    logger.info(f"Populating dim_time from {start_date} to {end_date}")

    # Generate date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]

    # Create DataFrame with time dimension attributes
    dim_time_data = []
    for date in date_list:
        dim_time_data.append([
            date.strftime("%Y-%m-%d"),  # date
            date.weekday() + 1,  # day_of_week (1-7, Mon-Sun)
            date.day,  # day_of_month
            date.timetuple().tm_yday,  # day_of_year
            date.isocalendar()[1],  # week_of_year
            date.month,  # month
            date.strftime("%B"),  # month_name
            (date.month - 1) // 3 + 1,  # quarter
            date.year,  # year
            date.weekday() >= 5,  # is_weekend (Sat/Sun)
            False  # is_holiday (simplified, could integrate holiday list)
        ])

    columns = [
        "date", "day_of_week", "day_of_month", "day_of_year", "week_of_year",
        "month", "month_name", "quarter", "year", "is_weekend", "is_holiday"
    ]
    dim_time_df = pd.DataFrame(dim_time_data, columns=columns)

    # Convert to list of tuples for bulk insert
    data_rows = [tuple(row) for row in dim_time_df.values]

    # Load into database
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Clear existing data (for idempotency in this demo)
        cursor.execute("TRUNCATE TABLE dim_time")

        # Insert new data
        query = f"""
            INSERT INTO dim_time (
                date, day_of_week, day_of_month, day_of_year, week_of_year,
                month, month_name, quarter, year, is_weekend, is_holiday
            ) VALUES %s
            ON CONFLICT (date) DO NOTHING
        """
        execute_values(cursor, query, data_rows)
        conn.commit()
        logger.info(f"Loaded {len(data_rows)} rows into dim_time")
        return len(data_rows)
    except Exception as e:
        logger.error(f"Failed to populate dim_time: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# Flyte task to transform data with business rules
@task(requests=Resources(cpu="1", mem="2Gi"))
def transform_data(df: pd.DataFrame, table_name: str, columns: List[str],
                   products_df: Optional[pd.DataFrame] = None,
                   orders_df: Optional[pd.DataFrame] = None) -> List[List[str]]:
    logger.info(f"Transforming data for {table_name}")

    # Specific business rules
    if table_name == "products" and products_df is not None:
        pass  # Join handled in DB schema

    elif table_name == "order_items":
        # Calculate revenue (price × quantity - discount)
        df["price"] = df["price"].astype(float)
        df["quantity"] = df["quantity"].astype(int)
        df["discount"] = df["discount"].astype(float)
        df["total"] = (df["price"] * df["quantity"]) - df["discount"]
        df = df.dropna(subset=columns)

    elif table_name == "customers" and orders_df is not None:
        # Enrich with total lifetime value
        orders_agg = orders_df.groupby("customer_id")["total_amount"].sum().reset_index()
        orders_agg = orders_agg.rename(columns={"total_amount": "lifetime_value"})
        df = df.merge(orders_agg, on="customer_id", how="left")
        df["lifetime_value"] = df["lifetime_value"].fillna(0).astype(float)
        df = df.dropna(subset=[col for col in columns if col != "lifetime_value"])  # Exclude lifetime_value from dropna

    else:
        # Common transformations for other tables
        df = df.dropna(subset=columns)

    # Type conversion after transformations
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(float if pd.api.types.is_float_dtype(df[col]) else int)
        df[col] = df[col].astype(str)

    # Convert to list of lists
    return [list(row) for row in df[columns].values]


# Flyte task to aggregate daily sales
@task(requests=Resources(cpu="1", mem="2Gi"))
def aggregate_daily_sales(order_items_df: pd.DataFrame, products_df: pd.DataFrame, orders_df: pd.DataFrame) -> List[
    List[str]]:
    logger.info("Aggregating daily sales by product and category")

    # Join order_items with orders to get order_date
    order_items_with_date = order_items_df.merge(
        orders_df[["order_id", "order_date"]],
        on="order_id",
        how="left"
    )

    # Convert order_date to datetime and extract date
    order_items_with_date["order_date"] = pd.to_datetime(order_items_with_date["order_date"])
    order_items_with_date["date"] = order_items_with_date["order_date"].dt.date.astype(str)
    order_items_with_date["product_id"] = order_items_with_date["product_id"].astype(int)

    # Join with products to get category_id
    merged = order_items_with_date.merge(
        products_df[["product_id", "category_id"]],
        on="product_id",
        how="left"
    )

    # Aggregate
    daily_agg = merged.groupby(["date", "product_id", "category_id"]).agg(
        units_sold=("quantity", "sum"),
        revenue=("total", "sum"),
        order_count=("order_id", "nunique")
    ).reset_index()
    daily_agg["avg_unit_price"] = (daily_agg["revenue"] / daily_agg["units_sold"]).fillna(0)

    columns = ["date", "product_id", "category_id", "units_sold", "revenue", "order_count", "avg_unit_price"]
    return [list(row) for row in daily_agg[columns].astype(str).values]


# Flyte task to load data into database
@task(requests=Resources(cpu="1", mem="1Gi"))
def load_to_db(table_name: str, columns: List[str], data_rows: List[List[str]]) -> int:
    logger.info(f"Loading data into {table_name}")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cursor, query, data_rows)
        conn.commit()
        logger.info(f"Loaded {len(data_rows)} rows into {table_name}")
        return len(data_rows)
    except Exception as e:
        logger.error(f"Load failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# Flyte task to refresh materialized view
@task(requests=Resources(cpu="1", mem="1Gi"))
def refresh_materialized_view(view_name: str, rows_loaded: int) -> bool:
    logger.info(f"Refreshing materialized view {view_name} after loading {rows_loaded} rows")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
        conn.commit()
        logger.info(f"Materialized view {view_name} refreshed")
        return True
    except Exception as e:
        logger.error(f"Refresh failed: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# Flyte workflow to orchestrate ETL
@workflow
def etl_workflow():
    data_configs = [
        {"file_path": "ecommerce_data/sample_product_categories.csv", "table_name": "product_categories",
         "columns": ["category_id", "name", "description", "created_at"]},
        {"file_path": "ecommerce_data/sample_products.csv", "table_name": "products",
         "columns": ["product_id", "name", "description", "price", "cost", "category_id", "sku", "inventory_count",
                     "weight", "created_at", "is_active"]},
        {"file_path": "ecommerce_data/sample_customers.csv", "table_name": "customers",
         "columns": ["customer_id", "email", "first_name", "last_name", "street_address", "city", "state", "zip_code",
                     "country", "phone", "registration_date", "last_login", "lifetime_value"]},
        {"file_path": "ecommerce_data/sample_orders.csv", "table_name": "orders",
         "columns": ["order_id", "customer_id", "order_date", "status", "payment_method", "shipping_address",
                     "shipping_city", "shipping_state", "shipping_zip", "shipping_country", "processing_date",
                     "shipping_date", "delivery_date", "total_amount"]},
        {"file_path": "ecommerce_data/sample_order_items.csv", "table_name": "order_items",
         "columns": ["order_item_id", "order_id", "product_id", "quantity", "price", "discount", "total"]}
    ]

    # Populate dim_time (run first as it’s independent)
    dim_time_rows_loaded = populate_dim_time(start_date="2021-01-01", end_date="2025-12-31")

    # Extract all data
    extracted_data = {config["table_name"]: extract_csv(file_path=config["file_path"]) for config in data_configs}

    # Concatenate chunks
    cat_df = concatenate_chunks(chunks=extracted_data["product_categories"])
    prod_df = concatenate_chunks(chunks=extracted_data["products"])
    cust_df = concatenate_chunks(chunks=extracted_data["customers"])
    ord_df = concatenate_chunks(chunks=extracted_data["orders"])
    oi_df = concatenate_chunks(chunks=extracted_data["order_items"])

    # Transform and load product_categories
    cat_transformed = transform_data(df=cat_df, table_name="product_categories", columns=data_configs[0]["columns"])
    cat_rows_loaded = load_to_db(table_name="product_categories", columns=data_configs[0]["columns"],
                                 data_rows=cat_transformed)

    # Transform and load products (depends on product_categories)
    prod_transformed = transform_data(df=prod_df, table_name="products", columns=data_configs[1]["columns"])
    prod_rows_loaded = load_to_db(table_name="products", columns=data_configs[1]["columns"],
                                  data_rows=prod_transformed)

    # Transform and load customers (must be before orders due to FK)
    cust_transformed = transform_data(df=cust_df, table_name="customers", columns=data_configs[2]["columns"],
                                      orders_df=ord_df)
    cust_rows_loaded = load_to_db(table_name="customers", columns=data_configs[2]["columns"],
                                  data_rows=cust_transformed)

    # Transform and load orders (depends on customers)
    ord_transformed = transform_data(df=ord_df, table_name="orders", columns=data_configs[3]["columns"])
    ord_rows_loaded = load_to_db(table_name="orders", columns=data_configs[3]["columns"],
                                 data_rows=ord_transformed)

    # Transform and load order_items (depends on orders and products)
    oi_transformed = transform_data(df=oi_df, table_name="order_items", columns=data_configs[4]["columns"])
    oi_rows_loaded = load_to_db(table_name="order_items", columns=data_configs[4]["columns"],
                                data_rows=oi_transformed)

    # Aggregate daily sales (depends on order_items, products, and orders)
    daily_agg = aggregate_daily_sales(order_items_df=oi_df, products_df=prod_df, orders_df=ord_df)
    daily_rows_loaded = load_to_db(table_name="daily_sales_aggregation",
                                   columns=["date", "product_id", "category_id", "units_sold", "revenue", "order_count",
                                            "avg_unit_price"],
                                   data_rows=daily_agg)

    # Refresh materialized view
    refresh_success = refresh_materialized_view(view_name="product_sales_summary", rows_loaded=oi_rows_loaded)


# Unit Tests
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "product_id": [1, 2], "name": ["A", "B"], "description": ["Desc A", "Desc B"],
        "price": [10.0, 20.0], "cost": [5.0, 10.0], "category_id": [1, 2],
        "sku": ["SKU1", "SKU2"], "inventory_count": [100, 200], "weight": [1.0, 2.0],
        "created_at": ["2023-01-01", "2023-01-02"], "is_active": [True, True]
    })


def test_extract_csv(tmp_path):
    file = tmp_path / "test.csv"
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df.to_csv(file, index=False)
    result = extract_csv(file_path=str(file))
    assert len(result) == 1
    pd.testing.assert_frame_equal(result[0], df)


def test_transform_data_products(sample_df):
    columns = ["product_id", "name", "description", "price", "cost", "category_id", "sku",
               "inventory_count", "weight", "created_at", "is_active"]
    result = transform_data(df=sample_df, table_name="products", columns=columns)
    expected = [list(row) for row in sample_df[columns].astype(str).values]
    assert result == expected


def test_transform_data_order_items():
    df = pd.DataFrame({"order_item_id": [1], "order_id": [1], "product_id": [1],
                       "quantity": [2], "price": [10.0], "discount": [1.0], "total": [0.0]})
    columns = ["order_item_id", "order_id", "product_id", "quantity", "price", "discount", "total"]
    result = transform_data(df=df, table_name="order_items", columns=columns)
    expected = [["1", "1", "1", "2", "10.0", "1.0", "19.0"]]  # (10 * 2) - 1 = 19
    assert result == expected


def test_load_to_db(mocker):
    mocker.patch("psycopg2.connect", return_value=mocker.Mock())
    conn = psycopg2.connect()
    cursor = conn.cursor.return_value
    data_rows = [["1", "test"], ["2", "test2"]]
    columns = ["id", "name"]
    result = load_to_db(table_name="test_table", columns=columns, data_rows=data_rows)
    assert result == 2
    cursor.execute.assert_called_once()


def test_populate_dim_time(mocker):
    mocker.patch("psycopg2.connect", return_value=mocker.Mock())
    conn = psycopg2.connect()
    cursor = conn.cursor.return_value
    result = populate_dim_time(start_date="2023-01-01", end_date="2023-01-03")
    assert result == 3  # 3 days
    cursor.execute.assert_called()


if __name__ == "__main__":
    logger.info("Starting ETL workflow...")
    etl_workflow()
    logger.info("ETL workflow completed. Keeping container alive...")
    while True:
        time.sleep(60)
        logger.info("Workflow service is still alive...")