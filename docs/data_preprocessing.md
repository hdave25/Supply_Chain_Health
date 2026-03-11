# Data Pre-processing

Below pre-processing have been done on the datasets given:

## 1. Normalizing date formats
To convert dates to standard format, use pd.to_datetime()
Conversion has been done for below fields:
* order_date (purchase_orders.csv)
* delivery_date (purchase_orders.csv)
* actual_delivery_date (shipments.csv)
* snapshot_date (inventory_snapshots.csv)

## 2. Handling missing values and invalid entries
Fill null quantities with zero and add a flag to indicate it was not available

* quantity (purchase_orders.csv)
* shipments_quantity (shipments.csv)

Fill missing delivery dates with median of vendor lead time
**Assumption**: If we consider median of vendor lead time, then it 
will be more near & close to actual data based on historical performace of vendor 

* delivery_date (purchase_orders.csv)

Remove records with invalid vendor_id (having BAD, null, '', invalid format)

* vendor_id (purchase_orders.csv)

## 3. Cleaning vendor_id, material_id, po_id, shipment_id

Checking formatting of vendor_id, material_id, po_id, shipment_id as per required format and remove entries if not aligning with format

* po_id (purchase_orders.csv)
* material_id (purchase_orders.csv)
* vendor_id (purchase_orders.csv)
* shipment_id (shipments.csv)
* material_id (inventory_snapshots.csv)
* plant_id (inventory_snapshots.csv)

