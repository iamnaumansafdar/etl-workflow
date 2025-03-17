from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from ariadne import QueryType, MutationType, gql, make_executable_schema
from ariadne.asgi import GraphQL
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname="ecommerce", user="admin", password="password", host="postgres", port="5432",
        cursor_factory=RealDictCursor
    )


# Load GraphQL schema
with open("schema.graphql", "r") as f:
    type_defs = gql(f.read())

# Define query and mutation resolvers
query = QueryType()
mutation = MutationType()


@query.field("productSales")
def resolve_product_sales(_, info, startDate, endDate, productId=None, categoryId=None,
                          limit=10, offset=0, sortBy="order_date", sortOrder="ASC"):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT o.order_id, o.customer_id, o.order_date, o.total_amount
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.order_date BETWEEN %s AND %s
        AND o.status NOT IN ('Cancelled', 'Returned')
        {product_filter}
        {category_filter}
        ORDER BY {sort_by} {sort_order}
        LIMIT %s OFFSET %s
    """
    filters = []
    params = [startDate, endDate]
    if productId:
        filters.append("oi.product_id = %s")
        params.append(productId)
    if categoryId:
        filters.append("p.category_id = %s")
        params.append(categoryId)

    query = query.format(
        product_filter="AND " + " AND ".join(filters) if filters else "",
        category_filter="",
        sort_by=f"o.{sortBy}" if sortBy in ["order_date", "total_amount"] else "o.order_date",
        sort_order=sortOrder if sortOrder in ["ASC", "DESC"] else "ASC"
    )
    cursor.execute(query, params + [limit, offset])
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"order_id": r["order_id"], "customer_id": r["customer_id"],
             "order_date": r["order_date"].isoformat(), "total_amount": float(r["total_amount"])}
            for r in results]


@query.field("customerPurchaseHistory")
def resolve_customer_purchase_history(_, info, customerId, startDate=None, endDate=None,
                                      limit=10, offset=0, sortBy="order_date", sortOrder="DESC"):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT order_id, customer_id, order_date, total_amount
        FROM orders
        WHERE customer_id = %s
        AND status NOT IN ('Cancelled', 'Returned')
        {date_filter}
        ORDER BY {sort_by} {sort_order}
        LIMIT %s OFFSET %s
    """
    params = [customerId]
    filters = []
    if startDate:
        filters.append("order_date >= %s")
        params.append(startDate)
    if endDate:
        filters.append("order_date <= %s")
        params.append(endDate)

    query = query.format(
        date_filter="AND " + " AND ".join(filters) if filters else "",
        sort_by=f"{sortBy}" if sortBy in ["order_date", "total_amount"] else "order_date",
        sort_order=sortOrder if sortOrder in ["ASC", "DESC"] else "DESC"
    )
    cursor.execute(query, params + [limit, offset])
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"order_id": r["order_id"], "customer_id": r["customer_id"],
             "order_date": r["order_date"].isoformat(), "total_amount": float(r["total_amount"])}
            for r in results]


@query.field("topSellingProductsByCategory")
def resolve_top_selling_products(_, info, categoryId, startDate=None, endDate=None,
                                 limit=10, sortBy="total_units_sold", sortOrder="DESC"):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT 
            p.product_id, 
            p.name AS product_name, 
            pc.name AS category_name, 
            SUM(oi.quantity) AS total_units_sold,
            SUM(oi.total) AS total_revenue,
            COUNT(DISTINCT o.order_id) AS order_count
        FROM products p
        JOIN product_categories pc ON p.category_id = pc.category_id
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE pc.category_id = %s
        AND o.status NOT IN ('Cancelled', 'Returned')
        {date_filter}
        GROUP BY p.product_id, p.name, pc.name
        ORDER BY {sort_by} {sort_order}
        LIMIT %s
    """
    params = [categoryId]
    filters = []
    if startDate:
        filters.append("o.order_date >= %s")
        params.append(startDate)
    if endDate:
        filters.append("o.order_date <= %s")
        params.append(endDate)

    query = query.format(
        date_filter="AND " + " AND ".join(filters) if filters else "",
        sort_by=sortBy if sortBy in ["total_units_sold", "total_revenue", "order_count"] else "total_units_sold",
        sort_order=sortOrder if sortOrder in ["ASC", "DESC"] else "DESC"
    )
    cursor.execute(query, params + [limit])
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"product_id": r["product_id"], "product_name": r["product_name"],
             "category_name": r["category_name"], "total_units_sold": r["total_units_sold"],
             "total_revenue": float(r["total_revenue"]), "order_count": r["order_count"]}
            for r in results]


@query.field("salesTrends")
def resolve_sales_trends(_, info, startDate, endDate, interval="day"):
    conn = get_db_connection()
    cursor = conn.cursor()
    interval_map = {"day": "day", "week": "week", "month": "month"}
    trunc_interval = interval_map.get(interval, "day")
    query = f"""
        SELECT 
            DATE_TRUNC(%s, dt.date) AS date,
            SUM(oi.total) AS total_sales
        FROM dim_time dt
        JOIN orders o ON DATE(o.order_date) = dt.date
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE dt.date BETWEEN %s AND %s
        AND o.status NOT IN ('Cancelled', 'Returned')
        GROUP BY DATE_TRUNC(%s, dt.date)
        ORDER BY DATE_TRUNC(%s, dt.date)
    """
    cursor.execute(query, (trunc_interval, startDate, endDate, trunc_interval, trunc_interval))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"date": r["date"].isoformat(), "total_sales": float(r["total_sales"])} for r in results]


@mutation.field("updateProduct")
def resolve_update_product(_, info, productId, name=None, price=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    updates = []
    params = []
    if name:
        updates.append("name = %s")
        params.append(name)
    if price is not None:  # Allow price to be 0
        updates.append("price = %s")
        params.append(price)

    if not updates:
        cursor.close()
        conn.close()
        raise ValueError("No fields provided to update")

    params.append(productId)
    query = f"""
        UPDATE products 
        SET {', '.join(updates)}, updated_at = NOW()
        WHERE product_id = %s
        RETURNING product_id, name, price, category_id
    """
    cursor.execute(query, params)
    result = cursor.fetchone()
    conn.commit()

    # Fetch category details
    cursor.execute("SELECT category_id, name FROM product_categories WHERE category_id = %s", (result["category_id"],))
    category = cursor.fetchone()

    cursor.close()
    conn.close()

    if not result:
        raise ValueError(f"Product with ID {productId} not found")

    return {
        "product_id": result["product_id"],
        "name": result["name"],
        "price": float(result["price"]),
        "category": {"category_id": category["category_id"], "name": category["name"]}
    }


# Create executable schema
schema = make_executable_schema(type_defs, [query, mutation])

# FastAPI app with CORS
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://studio.apollographql.com", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

graphql_app = GraphQL(schema, debug=True)

# Handle POST and GET requests to /graphql
app.mount("/graphql", graphql_app)

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)