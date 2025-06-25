# ✈️ Airline DBT Project

This project transforms raw airline operations data into clean, analytics-ready models using a Bronze → Silver → Gold architecture, with the end goal of supporting revenue, cost, and profit analysis per flight day. All models are materialized as **views** for fast iteration and lightweight inspection.

---

## Layered Architecture

### Bronze Layer – Staging

Lightly cleaned models aligned to raw source tables. Each model:
- Standardizes column names (`snake_case`)
- Applies basic casting and formatting (e.g., timestamps, lowercase enums)
- Preserves source grain

**Example models:**
- `stg_passenger_bookings`
- `stg_fuel_prices`
- `stg_flights`
- `stg_crew_payroll`
- `stg_plane_inventory`

---

### Silver Layer – Intermediate Logic

Enriched and partially aggregated business logic, bridging staging and final metrics. This layer begins incorporating assumptions used in analytics and reporting.

**Example models:**
- `int_booking_metrics`: Aggregates bookings per flight/day with revenue estimates
- `int_booking_revenue`: Flattens booking-level estimated revenue
- `int_fuel_costs_enriched`: Enriches fuel prices and categorizes fuel types
- `int_crew_costs`: Summarizes monthly crew payroll into total costs
- `int_plane_inventory_enriched`: Simplifies aircraft status data

---

### Gold Layer – Business-Facing Facts

High-level models used for P&L reporting and dashboarding. This layer combines revenue and cost models into unified profit snapshots by flight day.

**Example models:**
- `fct_revenue_summary`: Aggregated estimated revenue by day and booking status
- `fct_operational_costs`: Crew + fuel + aircraft operational costs by day
- `fct_flight_profit`: Flight-level P&L summary (revenue minus cost)

---

## Testing Strategy

Column-level testing is applied at each layer to ensure data integrity:

- **Staging (`bronze`)**
  - `not_null` on key columns like `booking_id`, `flight_id`
  - Safe casts with `try_cast`

- **Intermediate (`silver`)**
  - `not_null` and `unique` on `booking_id`, `flight_day` where relevant
  - Basic business logic validations (e.g., no negative revenue)

- **Gold (`gold`)**
  - `not_null` on all core metrics: `flight_day`, `total_revenue_usd`, `profit_usd`

Run tests via:

```bash
dbt test
```

---

## ⚙️ Materializations

All models are currently set as:

```yaml
+materialized: view
```

This supports fast iteration during development. Final models may be upgraded to `incremental` or `table` materializations if performance tuning is needed.

---

##  Naming & Schema Conventions

Schemas are separated by layer using environment-aware naming:

| Layer   | Example Schema      |
|---------|---------------------|
| Bronze  | `dbt_bronze`        |
| Silver  | `dbt_silver`        |
| Gold    | `dbt_gold`          |

Use `ref()` inside models to auto-resolve across layers regardless of schema.

---

## Future Improvements

- Add real dates and join keys to `crew`, `fuel`, and `inventory` for per-flight granularity
- Replace mocked cost assumptions with calculated values per aircraft/route
- Consider converting core models to `incremental` with freshness checks
- Add visual outputs via Superset, Streamlit, or pandas-based CLI tables

---

## Observations

- Fuel and payroll models currently use mocked dates and assumptions for demo purposes
- P&L outputs are functional but inflated due to placeholder crew/fuel costs
- Data quality needs attention before final visualizations

---
