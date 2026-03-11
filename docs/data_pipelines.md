# Data Pipelines

For data pipelines, medallion data architecture is followed.
#### There are mainly three data pipelines for this project:

1. Data Preprocessing pipeline
2. Raw data ingestion pipeline (bronze layer)
3. Business models pipeline (silver & gold layer)

### Data Preprocessing pipeline:

This pipeline does normalization, cleaning of data 
along with handling missing & invalid entries.
As part of output, it generates three csv files with cleaned data:

1. cleaned_purchase_orders.csv
2. cleaned_shipments.csv
3. cleaned_inventory_snapshots.csv

These files also available as part of APIs (please see api_usage.md)

### Raw data ingestion pipeline:

Following medallion data architecture, first, raw data is ingested to SQLite
database as is without any transformation but with timestamp of insertion.

Raw data is inserted **incrementally** with *delete+insert* strategy.

This will help us save historical data and access to it whenever
needed to debug issue/failure.

As output, three raw tables are created/updated:

1. raw_purchase_orders
2. raw_shipments
3. raw_inventory_snapshots

### Business models pipeline:

Under business models pipeline, we have silver and 
gold layer table that are created for 
KPI calculation, analytics-ready output and performance metrics.

As an output, four tables are produced with required
KPI calculation, metrics and overall health data:

1. supplier_performance_metrics
2. aggregated_vendor_kpis
3. material_inventory_risk
4. supply_chain_health