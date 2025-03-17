Below is a comprehensive README for your e-commerce analytics platform project. It includes instructions for running the project with and without `make`, guidance on using Apollo Sandbox for GraphQL on localhost, and example GraphQL queries. This assumes your project structure includes `workflow.py`, `app.py`, `database-schema.sql`, `docker-compose.yml`, and the provided Makefile-like script (interpreted as shell commands).

---

# E-Commerce Analytics Platform

This project implements an ETL pipeline and GraphQL API for an e-commerce analytics platform. It processes sales data from CSV files, loads it into a PostgreSQL database, and provides queryable insights via GraphQL. The pipeline is orchestrated with Flyte, and the system runs in Docker containers.

## Prerequisites
- **Docker**: Ensure Docker and Docker Compose are installed (`docker --version`, `docker-compose --version`).
- **Python 3.12**: For local development or running without Docker.
- **Flyte Cluster**: Optional for scheduled workflows (see "Scheduled Workflow" section).
- **Git**: To clone the repository.

## Project Structure
- `workflow.py`: Flyte ETL workflow for processing CSVs.
- `app.py`: FastAPI app with GraphQL API.
- `database-schema.sql`: PostgreSQL schema.
- `docker-compose.yml`: Docker configuration for services (`postgres`, `api`, `workflow`).
- `Dockerfile`: Builds the Python environment with Poetry.
- `ecommerce_data/`: Directory with sample CSVs (e.g., `sample_orders.csv`).

## Setup Instructions

### Option 1: Running with Shell Commands
The project includes a shell script (`/bin/bash`) with commands for building, starting, and stopping services.

#### Build the Project
```bash
bash -c "docker compose -f docker-compose.yml build"
```
- Builds Docker images for `postgres`, `api`, and `workflow`.

#### Start the Project
```bash
bash -c "docker compose -f docker-compose.yml up"
```
- Starts all services. The ETL runs once, GraphQL API is available at `http://localhost:8000/graphql`.

#### Stop the Project
```bash
bash -c "docker compose -f docker-compose.yml down --remove-orphans"
```
- Stops and removes containers, cleaning up orphaned resources.

### Option 2: Running without Shell Commands (Direct Docker Compose)
If you prefer not to use the shell script:

#### Build the Project
```bash
docker-compose -f docker-compose.yml build
```

#### Start the Project
```bash
docker-compose -f docker-compose.yml up
```

#### Stop the Project
```bash
docker-compose -f docker-compose.yml down --remove-orphans
```

### Local Development (No Docker)
1. Install dependencies:
   ```bash
   poetry install
   ```
2. Start PostgreSQL locally (e.g., via Docker or a local instance):
   ```bash
   docker run -d -p 5432:5432 -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=password -e POSTGRES_DB=ecommerce postgres:14
   ```
3. Load the schema:
   ```bash
   psql -h localhost -U admin -d ecommerce -f database-schema.sql
   ```
4. Run the ETL:
   ```bash
   python workflow.py
   ```
5. Run the API:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```

## Using Apollo Sandbox for GraphQL
Apollo Sandbox provides a UI to test GraphQL queries on `http://localhost:8000/graphql`.

1. **Start the API:**
   - Use either method above to start the project. Ensure the `api` service is running.
   
2. **Open Apollo Sandbox:**
   - Visit [Apollo Studio](https://studio.apollographql.com/sandbox/explorer).
   - Set the endpoint to `http://localhost:8000/graphql`.

3. **Test Queries:**
   - Use the examples below in the Sandbox UI. CORS is configured to allow `https://studio.apollographql.com`.

## Example GraphQL Queries

### 1. Product Sales
Retrieve orders within a date range, optionally filtered by product or category.
```graphql
query {
  productSales(startDate: "2023-01-01", endDate: "2023-12-31", productId: 1, limit: 5) {
    order_id
    customer_id
    order_date
    total_amount
  }
}
```

### 2. Customer Purchase History
Get a customer’s orders with sorting and pagination.
```graphql
query {
  customerPurchaseHistory(customerId: 1, startDate: "2023-01-01", limit: 3, sortOrder: "DESC") {
    order_id
    order_date
    total_amount
  }
}
```

### 3. Top Selling Products by Category
List top products in a category by units sold.
```graphql
query {
  topSellingProductsByCategory(categoryId: 1, limit: 5, sortBy: "total_units_sold") {
    product_id
    product_name
    category_name
    total_units_sold
    total_revenue
    order_count
  }
}
```

### 4. Sales Trends
Aggregate sales over time by day, week, or month.
```graphql
query {
  salesTrends(startDate: "2023-01-01", endDate: "2023-12-31", interval: "month") {
    date
    total_sales
  }
}
```

### 5. Update Product (Mutation)
Update a product’s name and price.
```graphql
mutation {
  updateProduct(productId: 1, name: "Updated Product", price: 29.99) {
    product_id
    name
    price
    category {
      category_id
      name
    }
  }
}
```

## Troubleshooting
- **Database Connection Errors:** Ensure `postgres` is running and environment variables match (`DATABASE_HOST=postgres`).
- **CSV Missing:** Place sample CSVs in `ecommerce_data/` (e.g., `sample_orders.csv`).
- **GraphQL Not Responding:** Verify `api` service is up (`http://localhost:8000/graphql`).

## Notes
- Sample CSVs must exist in `ecommerce_data/` for the ETL to process data.
- The project runs locally via Docker Compose; Flyte scheduling requires a cluster.
