## Business Logic

Here is the documentation of business logics
used for building silver & gold layer tables:

### *supplier_performance_metrics*

In building supplier performance metrics, we have used 
raw_purchase_orders and raw_shipments data as sources.

Both the tables are joined based on *po_id* which 
is the unique key for raw_purchase_orders.

**Calculations of KPIs:**

*delivery_delta*: It is the day difference between actual delivery date
and planned_delivery_date.

Code: 
```sql
JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date)
```

*delivery_delay_days*: It is the positive difference between actual_delivery_date
and planned_delivery_date. If shipment is completed on time or before, then value 
will be 0.

Code: 

```sql
CASE
    WHEN (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date)) > 0
    THEN (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date))
    ELSE 0
END AS delivery_delay_days,
```

*delivery_status*: 'On Time' or 'Late'

If actual_delivery_date <= planned_delivery_date then, 'On Time' <br>
If actual_delivery_date > planned_delivery_date then, 'Late'

### *aggregated_vendor_kpis*

For aggregated vendor KPIs, supplier_performance_metrics
table is used and latest snapshot of the table is taken everytime for calculation.

*on_time_delivery_percentage*: On Time Delivery Percentage is calculated
based on 'On Time' or 'Late' shipment.

Code: 
```sql
ROUND(
    AVG(
        CASE
            WHEN actual_delivery_date <= planned_delivery_date THEN 100.0
            ELSE 0
        END
    ), 2
) AS on_time_delivery_percentage,
```
*avg_delay_days*: Average delay of days in a shipment by vendor.

### *material_inventory_risk*

This table stores inventory risk data for all the materials along with 
their current_stock, safety_stock, stock_margin etc. details.

*stock_margin*: Stock Margin = Current Stock - Safety Stock

*pct_time_below_safety*: Percentage of times when a material was below safety stock.

```sql
ROUND((sbs.times_below_safety * 100.0 / sbs.total_snapshots), 2) AS pct_time_below_safety,
```

*current_inventory_status*: 
<br>Below Safety Stock = When current_stock is less than safety_stock
<br>Near Safety Stock = When current stock is less than 120% of safety_stock
<br>Adequate Stock = When current_stock is more than 120% of safety_stock
```sql
CASE
    WHEN ls.current_stock < ls.safety_stock THEN 'Below Safety Stock'
    WHEN ls.current_stock < (ls.safety_stock * 1.2) THEN 'Near Safety Stock'
    ELSE 'Adequate Stock'
END AS current_inventory_status,
```

*risk_classification*: Inventory risk classification.
<br>High Risk: When a material falls below safety_stock 
more than or equal to 20% in all snapshots, then it is 'High Risk'.
<br>Medium Risk: When a material falls below safety_stock 
more than or equal to 10% but less than 20% in all snapshots, then it is 'Medium Risk'.
<br>Low Risk: When a material falls below safety_stock 
less than 10% in all snapshots, then it is 'Low Risk'.



```sql
CASE
    WHEN (sbs.times_below_safety * 100.0 / sbs.total_snapshots) >= 20 THEN 'High Risk'
    WHEN (sbs.times_below_safety * 100.0 / sbs.total_snapshots) >= 10 THEN 'Medium Risk'
    ELSE 'Low Risk'
END AS risk_classification,
```

### *supply_chain_health*

For supply_chain_health table, all the KPIs documented
above are combined and produced as a single view.