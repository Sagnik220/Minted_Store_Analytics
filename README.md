<div align="center">

<img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=delta&logoColor=white"/>
<img src="https://img.shields.io/badge/Shopify-96BF48?style=for-the-badge&logo=shopify&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>

# 🏪 Minted Store — End-to-End Data Warehouse

### A production-grade Lakehouse built entirely on Databricks Community Edition
**Shopify + Shiprocket → Delta Lake Medallion Architecture → Gold Business Intelligence**

*Real APIs. Real data. Real collectibles. Zero cloud spend.*

</div>

---

## 📖 Overview

[Minted Store](https://www.mintedstore.in/) is an Indian e-commerce business specialising in premium collectibles — Funko Pops, Hot Wheels, Gundam kits, Beyblade, and licensed anime figures from Bandai, Mattel, Hasbro, Funko, and Takara Tomy.

This repository contains the **internal Data Warehouse** powering Minted Store's analytics operations — ingesting live order, fulfilment, and inventory data from Shopify and Shiprocket, transforming it through a Delta Lake Medallion Architecture, and delivering business intelligence across six analytical domains to support day-to-day operations and strategic decisions.

---

## 🏗️ Architecture

![Minted Store Analytics Architecture](https://raw.githubusercontent.com/Sagnik220/Minted_Store_Analytics/main/docs/architecture.png)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
│                                                                         │
│   ┌──────────────────────┐         ┌──────────────────────┐            │
│   │   Shopify Admin API  │         │   Shiprocket API     │            │
│   │  8 endpoints         │         │  6 endpoints         │            │
│   │  Cursor pagination   │         │  JWT auth + paging   │            │
│   │  Incremental loads   │         │  Per-AWB tracking    │            │
│   └──────────┬───────────┘         └──────────┬───────────┘            │
└──────────────┼──────────────────────────────── ┼───────────────────────┘
               │                                 │
               ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS COMMUNITY EDITION                         │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  🥉 BRONZE LAYER  (Raw — Delta Lake, partitioned by ingestion date)│ │
│  │                                                                    │ │
│  │  minted_bronze.shopify_orders          (cursor paginated)          │ │
│  │  minted_bronze.shopify_products        (cursor paginated)          │ │
│  │  minted_bronze.shopify_customers       (cursor paginated)          │ │
│  │  minted_bronze.shopify_inventory_levels(location-scoped)           │ │
│  │  minted_bronze.shopify_price_rules     (discount rules)            │ │
│  │  minted_bronze.shopify_refunds         (nested → flattened)        │ │
│  │  minted_bronze.shopify_fulfillments    (nested → flattened)        │ │
│  │  minted_bronze.shiprocket_orders       (page paginated)            │ │
│  │  minted_bronze.shiprocket_shipments    (AWB codes, courier data)   │ │
│  │  minted_bronze.shiprocket_tracking     (per-AWB fetch)             │ │
│  │  minted_bronze.shiprocket_ndr          (Non-Delivery Reports)      │ │
│  │  minted_bronze.shiprocket_returns      (return orders)             │ │
│  │                                                                    │ │
│  │  + ingestion_audit_log  + ingestion_watermarks                     │ │
│  └──────────────────────────────┬─────────────────────────────────────┘ │
│                                 │  PySpark from_json() + MERGE INTO     │
│  ┌──────────────────────────────▼─────────────────────────────────────┐ │
│  │  🥈 SILVER LAYER  (Cleaned, typed, deduped, IST timestamps)        │ │
│  │                                                                    │ │
│  │  minted_silver.shopify_orders           (typed + festival flags)   │ │
│  │  minted_silver.shopify_order_line_items (exploded from orders)     │ │
│  │  minted_silver.shopify_products         (brand + limited_ed flag)  │ │
│  │  minted_silver.shopify_customers        (PII masked, tier scored)  │ │
│  │  minted_silver.shopify_inventory_levels (stock_status derived)     │ │
│  │  minted_silver.shiprocket_shipments     (delivery KPIs derived)    │ │
│  └──────────────────────────────┬─────────────────────────────────────┘ │
│                                 │  PySpark SQL Aggregations             │
│  ┌──────────────────────────────▼─────────────────────────────────────┐ │
│  │  🥇 GOLD LAYER   (Business aggregates — 6 analytical domains)      │ │
│  │                                                                    │ │
│  │  minted_gold.sales_daily            Revenue, AOV, fulfillment rate │ │
│  │  minted_gold.brand_performance      Funko vs HotWheels vs Bandai   │ │
│  │  minted_gold.customer_rfm           RFM segments + tier scoring    │ │
│  │  minted_gold.inventory_health       Stock risk + sold-out velocity │ │
│  │  minted_gold.logistics_performance  Courier KPIs, RTO/NDR rates    │ │
│  │  minted_gold.cohort_ltv             Monthly cohort lifetime value  │ │
│  └──────────────────────────────┬─────────────────────────────────────┘ │
└─────────────────────────────────┼───────────────────────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   Databricks SQL Dashboards  │
                    │   (or Metabase / Superset)   │
                    └─────────────────────────────┘
```

---

## 📁 Project Structure

```
minted-dw/
│
├── notebooks/
│   ├── 00_setup/
│   │   ├── 00_config.py              ← Central config: API creds, DBFS paths, endpoints
│   │   ├── 00_utils.py               ← Shared: HTTP client, paginators, watermarks, audit
│   │   └── 00_init_databases.py      ← One-time: creates all databases + Delta tables
│   │
│   ├── 01_shopify_bronze/
│   │   └── 01_shopify_ingest_all.py  ← Ingests all 8 Shopify endpoints → Bronze
│   │
│   ├── 02_shiprocket_bronze/
│   │   └── 02_shiprocket_ingest_all.py ← Ingests all 6 Shiprocket endpoints → Bronze
│   │
│   ├── 03_silver/
│   │   └── 03_silver_transforms.py   ← Bronze → Silver: parse, type, dedupe, enrich
│   │
│   ├── 04_gold/
│   │   └── 04_gold_aggregates.py     ← Silver → Gold: 6 business aggregate tables
│   │
│   └── 03_master_pipeline.py         ← Orchestrates all layers end-to-end
│
└── docs/
    └── architecture.png              ← Architecture diagram
```

---

## 🔌 Data Sources & Endpoints

### Shopify Admin API (8 Endpoints)

| Entity | Endpoint | Strategy | Key Insight |
|---|---|---|---|
| `orders` | `/orders.json` | Incremental (`updated_at_min`) | Nested line_items exploded in Silver |
| `products` | `/products.json` | Incremental | Brand + limited_edition flag derived from tags |
| `customers` | `/customers.json` | Incremental | PII masked at Silver; email_domain kept |
| `inventory_levels` | `/inventory_levels.json` | Full refresh | Requires location_ids pre-fetch |
| `price_rules` | `/price_rules.json` | Incremental | Powers discount analytics |
| `discounts` | `/discount_codes.json` | Full refresh | Linked to price_rules |
| `refunds` | Nested in `/orders.json` | Full refresh | Flattened with parent order_id |
| `fulfillments` | Nested in `/orders.json` | Full refresh | Flattened with parent order_id |

All Shopify endpoints use **cursor-based pagination** via the `Link` response header — not page numbers. This is a common junior DE mistake; this project handles it correctly.

### Shiprocket API (6 Endpoints)

| Entity | Endpoint | Strategy | Key Insight |
|---|---|---|---|
| `orders` | `/orders` | Incremental | Shiprocket order records |
| `shipments` | `/shipments` | Incremental | AWB codes, courier assignments |
| `tracking` | `/courier/track/awb/{awb}` | Per-AWB | Fetched individually after shipments |
| `couriers` | `/courier/courierListWithCounts` | Full refresh | Courier performance benchmarking |
| `ndr` | `/ndr/all` | Full refresh | Non-Delivery Reports — key for RTO analysis |
| `returns` | `/orders/processing/return` | Full refresh | Return order lifecycle |

Shiprocket uses **JWT authentication** that expires every 24h. Token is cached to DBFS and auto-refreshed before expiry.

---

## 🧊 Medallion Architecture Details

### 🥉 Bronze — Raw Ingestion
- Every record stored as `_raw STRING` (raw JSON) — **100% API fidelity**
- Schema changes in the Shopify/Shiprocket API will **never break Bronze writes**
- Partitioned by `_partition_date` (ingestion date) for efficient time-travel queries
- Metadata columns: `_source`, `_entity`, `_ingested_at`, `_run_id`, `_partition_date`
- Watermarks tracked in `minted_bronze.ingestion_watermarks` Delta table
- Every run logged in `minted_bronze.ingestion_audit_log`

### 🥈 Silver — Typed & Cleaned
- `from_json()` parses `_raw` → strongly typed columns
- `MERGE INTO` upserts on primary key — no duplicates
- All timestamps converted to **IST (Asia/Kolkata)** — no UTC confusion in dashboards
- PII handling: customer emails dropped; `email_domain` retained for analytics
- Derived columns: `is_festival_period`, `is_limited_edition`, `stock_status`, `is_rto`
- Line items **exploded** from nested orders into a flat fact table

### 🥇 Gold — Business Aggregates
Six purpose-built analytical tables, each answering a core business question:

| Table | Business Question |
|---|---|
| `sales_daily` | How much did we sell today vs last week? Is it a festival week? |
| `brand_performance` | Which brand — Funko or Hot Wheels — is driving revenue this month? |
| `customer_rfm` | Who are our Champions? Who's at risk of churning? |
| `inventory_health` | Which SKUs are about to sell out? |
| `logistics_performance` | Which courier has the best delivery rate and lowest RTO? |
| `cohort_ltv` | What is the 6-month LTV of customers acquired in October (Diwali)? |

---

## 📊 Analytics Domains

### 1. Sales Intelligence
- Revenue by day / week / month with WoW and MoM comparisons
- Average Order Value (AOV) trend — are customers buying more per order?
- Discount impact: net revenue vs gross revenue
- **Indian festival calendar overlay** — Diwali, Durga Puja, Comic Con, Republic Day spikes

### 2. Brand & Product Performance
- Funko vs Hot Wheels vs Bandai vs Mattel vs Hasbro — head-to-head monthly
- Limited edition revenue as % of total — do exclusive drops move the needle?
- Sold-out velocity — which SKUs vanish fastest after listing?
- Discount depth by brand — who gets the deepest markdowns?

### 3. Customer Analytics (RFM)
- **RFM Segmentation**: Champions, Loyal Customers, Promising, At Risk, Lost
- Customer tier: Whale (₹10k+), High Value, Mid Value, Low Value
- Repeat purchase rate and average inter-order gap
- Geographic distribution across Indian states

### 4. Inventory Intelligence
- Real-time stock risk scoring: 🔴 Sold Out → 🟡 Critical → 🟠 Low → 🟢 Healthy
- Days-of-cover estimation per SKU
- Stockout frequency analysis — lost revenue from unavailability

### 5. Logistics & Fulfillment (Shiprocket)
- Delivery rate, RTO rate, NDR rate — per courier per month
- Average days-to-deliver by courier and destination state
- Cost per shipment by courier — optimize carrier selection
- Non-Delivery Report (NDR) root cause analysis

### 6. Cohort LTV Analysis
- Monthly acquisition cohorts with 1/3/6/12-month revenue curves
- Festival cohort vs organic cohort LTV comparison
- Payback period estimation by customer segment

---

## ⚙️ Technical Stack

| Layer | Technology | Why |
|---|---|---|
| Compute | Databricks Community Edition | Free Spark + Delta Lake + notebooks |
| Storage Format | Delta Lake | ACID transactions, time travel, schema evolution |
| Ingestion | Python + `requests` | Full control over pagination, retries, auth |
| Transformation | PySpark + SQL | Distributed processing; standard in FAANG |
| Orchestration | Databricks Jobs | Native CE scheduling (cron) |
| Auth Management | DBFS-cached JWT | Avoids Shiprocket re-auth on every run |

---

## 🚀 Setup & Running

### Prerequisites
- [Databricks Community Edition](https://community.cloud.databricks.com) account (free)
- Shopify Admin API token with scopes: `read_orders`, `read_products`, `read_customers`, `read_inventory`
- Shiprocket account with API access (email + password)

### Step 1: Clone & Configure
```bash
git clone https://github.com/YOUR_USERNAME/minted-dw.git
cd minted-dw
```

Edit `notebooks/00_setup/00_config.py` with your credentials:
```python
SHOPIFY_CONFIG = {
    "shop_name":    "mintedstore",
    "access_token": "shpat_YOUR_TOKEN_HERE",
    ...
}
SHIPROCKET_CONFIG = {
    "email":    "your@email.com",
    "password": "your_password",
}
```
> ⚠️ `00_config.py` is in `.gitignore` — it will never be committed.

### Step 2: Import Notebooks into Databricks
In Databricks CE: **Workspace → Import → File** — import all `.py` files maintaining the folder structure.

### Step 3: One-Time Initialization
Run `notebooks/00_setup/00_init_databases.py`. This creates:
- `minted_bronze`, `minted_silver`, `minted_gold` databases
- All DBFS directories under `dbfs:/minted_dw/`
- Watermark tracking Delta table
- Audit log Delta table

### Step 4: Run the Pipeline
```
# Manual run:
Open notebooks/03_master_pipeline.py → Run All

# Scheduled run (recommended):
Databricks → Workflows → Jobs → Create Job
  Task:     notebooks/03_master_pipeline.py
  Schedule: 0 30 20 * * ?   (2:00 AM IST daily)
```

---

## 🔑 Key Engineering Decisions

**Raw JSON in Bronze, never parsed schemas**
Shopify and Shiprocket change their API response shapes. Storing `_raw STRING` means Bronze never breaks on upstream schema changes. Silver handles schema enforcement with `from_json()` and StructType.

**Watermarks in Delta, not in memory**
Watermarks survive cluster restarts. On Community Edition, clusters auto-terminate after 2 hours of inactivity. If the pipeline breaks mid-run, the next run picks up exactly where it left off.

**IST timestamps everywhere in Silver+**
All UTC timestamps are converted to `Asia/Kolkata` at the Silver layer. Dashboard consumers never deal with timezone math. This is a subtle but critical decision for an Indian business.

**Shopify cursor pagination, not page-based**
Shopify deprecated page-number pagination. The correct approach is parsing the `Link: <url>; rel="next"` response header to extract `page_info` cursors. This project implements it correctly.

**PII dropped at Silver, not Gold**
Customer emails and phone numbers are dropped at the Silver transformation step — not just excluded from Gold aggregates. This ensures PII is never present in any downstream table, simplifying compliance.

**Chunked writes for Community Edition**
CE runs on a single-node cluster with limited memory. Records are written in chunks of 1,000 (Shopify) and 500 (Shiprocket) to avoid OOM errors on large API pulls.

---

## 🗓️ Pipeline Schedule

```
Daily at 2:00 AM IST
─────────────────────────────────────────────────────────

Step 1 [~15 min]  Shopify Bronze Ingestion
  ├── orders         (incremental)
  ├── products       (incremental)
  ├── customers      (incremental)
  ├── inventory_levels (full refresh)
  ├── price_rules    (incremental)
  ├── discounts      (full refresh)
  ├── refunds        (full refresh)
  └── fulfillments   (full refresh)

Step 2 [~10 min]  Shiprocket Bronze Ingestion
  ├── orders         (incremental)
  ├── shipments      (incremental)
  ├── couriers       (full refresh)
  ├── ndr            (full refresh)
  ├── returns        (full refresh)
  └── tracking       (per-AWB, depends on shipments)

Step 3 [~10 min]  Silver Transforms
  ├── Parse JSON → typed columns
  ├── MERGE INTO (upsert on PK)
  ├── Derive: festival flags, brand, stock_status, RTO
  └── Drop PII

Step 4 [~5 min]   Gold Aggregates
  ├── sales_daily
  ├── brand_performance
  ├── customer_rfm     (RFM + tier scoring)
  ├── inventory_health
  ├── logistics_performance
  └── cohort_ltv

Total: ~40 minutes end-to-end
```

---

## 📈 Dashboard Coverage

Once Gold tables are populated, connect **Databricks SQL** or **Metabase OSS** to visualise:

| Dashboard | Key Metrics |
|---|---|
| **Executive Overview** | Daily revenue, AOV, order count, fulfillment rate, festival uplift |
| **Brand Deep-Dive** | Brand revenue share, units sold, limited edition %, discount rate |
| **Customer Health** | RFM segment distribution, repeat rate, churn risk, geographic map |
| **Inventory Risk** | Sold-out SKUs, critical stock, days-of-cover heatmap |
| **Logistics Report** | Courier comparison, delivery rate, RTO %, avg delivery days |
| **Cohort LTV** | Cohort curves, Diwali vs regular cohort LTV, payback period |

---

## 🌱 Roadmap

- [ ] **Silver**: Product variant-level table (size, color, edition)
- [ ] **Gold**: Cross-brand affinity (customers who buy Funko also buy \_\_\_)
- [ ] **Streaming**: Simulate real-time order events with Spark Structured Streaming
- [ ] **ML**: Churn prediction model on RFM features (MLflow in Databricks CE)
- [ ] **Alerting**: Notebook-triggered email on pipeline failure via Databricks webhooks
