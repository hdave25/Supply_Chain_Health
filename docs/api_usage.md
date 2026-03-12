# API Usage

Here is how you can use APIs to get required data:

### Documentation:
Use below APIs to get documentation.
```
/docs/api_usage.md
/docs/data_preprocessing.md
/docs/data_pipelines.md
/docs/business_logic.md
```

### Preprocessed Data:
Use below APIs to get preprocessed data in csv format.
```
/data_processed/cleaned_inventory_snapshots.csv
/data_processed/cleaned_purchase_orders.csv
/data_processed/cleaned_shipments.csv
```

### Business models & KPIs
Use below APIs to explore analytics ready data and
health of supply chain.

```/overall_vendor_performance``` : This API returns vendor performance data for all vendors.
<br>```/overall_material_risk``` : This API returns overall material risk insights for all materials.
<br>```/vendor_performance?vendor_id=``` : This API returns structured supplier performance metrics.
<br>```/material_risk?material_id=``` : This API returns current inventory risk and health for a particular material.
<br>```/health_summary``` : This API returns aggregated KPIs for dashboards.
<br>```/supplier_performance_metrics``` : This API returns overall supplier performance metrics.
<br>```/analytics/supply_chain_health.csv``` : This API returns structured data of overall supply_chain_health table.
<br>```/analytics/dashboard_ready.csv``` : This API returns dashboard ready data of overall supply chain metrics.
