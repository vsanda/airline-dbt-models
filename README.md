# ✈️ Airline DBT Project

This project simulates and transforms airline operations data into clean, analytics-ready models using a **Bronze → Silver → Gold** architecture. It supports granular flight-level profitability analysis with realistic-feeling synthetic data. All models are currently materialized as **views** for fast iteration and lightweight inspection.

---

## 🧱 Layered Architecture

### Bronze Layer – Raw Staging

Staging models align closely with raw input schemas. Each model:
- Standardizes column names (`snake_case`)
- Applies basic type casting (e.g., dates, booleans, enums)
- Retains source-level grain (e.g., one row per booking)

**Examples:**
- `stg_bookings`
- `stg_fuel_prices`
- `stg_flights`
- `stg_crew_payroll`
- `stg_supplier_logs`

---

### Silver Layer – Business Logic

These models enrich, join, and reshape staged data. Includes intermediate calculations used in final P&L logic.

**Examples:**
- `int_revenue_metrics`: Booking-level revenue incl. upsells
- `int_flights_metrics`: Enriching flight metrics with aircraft efficiency
- `int_fuel_metrics`: Fuel cost logic based on route distance + oil price
- `int_crew_metrics`: Aggregated monthly crew payroll per flight
- `int_delayed_events`: Delay penalties for late flights
- `int_supplier_metrics`: Vendor-driven costs like catering, cleaning

---

### Gold Layer – Flight Profitability Models

Final, business-facing fact models. These combine revenue and multiple cost categories to calculate flight- and route-level margins.

**Examples:**
- `profitability_summary`: Flight-level revenue, cost, and profit breakdowns
- `route_summary`: Avg. profit per route, with delay aggregation
- `operational_cost_summary`: Unified cost breakdown (crew, fuel, supplier, delay)
- `revenue_summary`: Flight revenue from bookings and upsells

---

## ✅ Testing Strategy

Column-level tests applied throughout:
- `not_null` and `unique` where required
- Business rule validations on cost/revenue fields
- Ensure safe casting via `try_cast` where applicable

```bash
dbt test
