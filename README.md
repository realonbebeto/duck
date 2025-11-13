# Duck Server
Duck Platform – The market data platform for consumer brands

## Prerequisites
✅ Have uv installed through: <a href="https://docs.astral.sh/uv/getting-started/installation/#__tabbed_1_1">installation docs</a>

## Server Setup

### 1. Clone the repository
Open your terminal and run:

`git clone https://github.com/realonbebeto/duck.git`

or

`git clone git@github.com:realonbebeto/duck.git`


Then navigate into the project directory:

`cd duck`

### 2. Create & activate a virtual environment
Setup environment by running:

```uv sync ```

Activate the virtual environment on unix:

```source .venv/bin/activate```

or on windows

```.venv\Scripts\activate```

### 3. Start the server
```uvicorn src.main:app --reload --host 0.0.0.0 --port 8001 --proxy-headers --workers 1```


Visit  http://127.0.0.1:8000  to access the server locally.

For the docs, visit  http://127.0.0.1:8001/docs  to access.


### 4. Installing a New Package/Library
```uv add <pypi-package-name>```


## Key Decisions
1. Ingest endpoint for processing new flat files (csv) and ingesting them
2. Saving data to (staging) with hash value to handle duplicate records from ingestion
3. Validating data to (production table - duck_store)
4. Saving validation errors for review(human in the loop perspective)


## Missing Vital Implementation 
1. Idempotency: proper reruns/requests with contistent ingestion minus duplicates
2. Self-healing: retry mechanism when a service/process fails due to server problem with human intervention
3. Tracing and Error handling to log error spans and causes
4. Context handling: Reports generated should only be for the new data ingested
   - As well provision to merge/incorporate new and old data for reports

##  🔧 Assumptions
1. Anyone other than Bidco is collectively considered a competitor
2. Weeks start on Monday
3. Customer behaviour is constant across the span of the data
4. Red flag thresholds are between 0.1  and 0.2 which translates to 10% and 20%
5. All sales uplift due noticed is due to promotion
6. All suppliers supply items within the same price range i.e.
7. Price volatility flagged when std dev > 2× mean price
8. "Unreliable" = >10% data issues (zeros, negatives, missing values)

**Thresholds are intentionally conservative** - adjustment in code is encouraged based on your business context (e.g., grocery vs electronics have different norms).

---

## 📍 Endpoints Overview

### 1. **POST `/api/ingest`** - Data Ingestion
**What it does:** Loads sales transaction data from CSV into DuckDB for analysis.

**Expected CSV format:**
```
Store Name, Item_Code, Item Barcode, Description, Category, Department, Sub-Department, Section	Quantity, Total Sales, RRP, Supplier, Date Of Sale,
```

**What "good" looks like:**
- ✅ All rows ingested successfully
- ✅ No missing required columns
- ✅ 0 validation errors

**Sample use case:**
*"Every morning at 6 AM, the data team uploads yesterday's sales extract from the POS system to refresh analytics."*

---

### 2. **POST `/api/errors`** - Data Ingestion Errors
**What it does:** Retrieves data specific errors providing context on location of errors.

**Checks:**
- Uniqueness thus no duplicates
- Values are not less than 0
- No future dates
- Non null values

**What "good" looks like:**
- ✅ All rows ingested successfully
- ✅ No missing required columns
- ✅ 0 validation errors

**Use case:**
*"The data team uploads retrieves errors to isolate incident locations"*

---

### 3. **GET `/api/data-quality`** - Data Quality Report
**What it does:** Identifies unreliable stores and suppliers based on data anomalies.

**Quality checks:**
- Missing data (zero sales, null values)
- Negative quantities or revenue
- Extreme price volatility (std dev > 2x average price)
- Low store coverage by supplier

**What "good" looks like:**
- ✅ <10% of total stores flagged as unreliable(thus needs attention)
- ✅ No/Few issues listed
- ✅ Overall health status: "good"

**Use case:**
*"A analytics manager runs to identify which stores need POS system checks or staff retraining. Store #Bidco showing 35% missing data triggers an urgent IT ticket."*

**Action triggers:**
- **Needs attention** → Immediate investigation required
- **Multiple zero-sales** → Check if store was closed or system down

---

### 4. **GET `/api/promotion-summary`** - Promotion Uplift Analysis
**What it does:** Measures promotional effectiveness by comparing promo period sales vs. baseline (historical non-promo average).

**Key metrics:**
- **Uplift %** = ((Promo Units - Baseline Units) / Baseline Units) × 100

**What "good" looks like:**
- ✅ Average uplift: 20-50% (category dependent)
- ✅ 80%+ of promos show positive uplift

**Use case:**
*"The category manager reviews promotions. Supplier A's 40% off promo delivered 65% volume lift (GOOD), while Supplier B's 30% off only got 8% lift (POOR) - signals price insensitivity or poor execution."*

**Red flags:**
- Negative uplift → Promo cannibalizing regular sales
- <10% uplift with >30% discount → Waste of margin

**Optional parameters:**
- `?min_uplift=25` - Filter to only show promos above 25% lift

---

### 5. **GET `/api/pricing-index`** - Competitive Pricing Report
**What it does:** Compares a target supplier's average unit prices vs. competitors within the same section/sub-department per store.

**Price Index interpretation:**
- **100** = Market average (competitive)
- **<95** = Discount position (undercutting competitors)
- **>105** = Premium position (charging above market)

**What "good" looks like:**
- ✅ Price index 95-105 for value brands (competitive)
- ✅ Price index 105-115 for premium brands (justified premium)
- ✅ Consistent positioning across stores

**Use case:**
*"The commercial director reviews Supplier X's pricing. Store A shows index of 92 (too cheap - leaving money on table), while Store B shows 118 (too expensive - likely losing volume). Both need repricing."*

**Required parameter:**
- `?supplier=SupplierName` - The supplier to analyze

**Business actions:**
- **Index 85-94** → Opportunity to raise prices 5-10%
- **Index 106-115** → Acceptable premium for quality brands
- **Index >120** → Risk of customer defection to competitors
- **Wide variance across stores** → Inconsistent pricing strategy needs correction

---


*Built with FastAPI + DuckDB | Optimized for retail speed-to-insight*