import sqlite3
from datetime import datetime
import pandas as pd


def ingest_raw_purchase_orders(cleaned_df_po):

    # Add the insert_ts column with current timestamp
    cleaned_df_po['insert_ts'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    table_name = "raw_purchase_orders"
    conn = sqlite3.connect('supply_chain.db')
    cursor = conn.cursor()

    try:
        # Check if table exists
        cursor.execute(
            f"SELECT name FROM sqlite_master "
            f"WHERE type='table' AND name='{table_name}'"
        )
        if cursor.fetchone():
            # Delete existing records to prevent duplicates and allow updates
            po_ids = cleaned_df_po['po_id'].tolist()
            query = f"DELETE FROM {table_name} WHERE po_id IN ({','.join(['?'] * len(po_ids))})"
            cursor.execute(query, po_ids)
            print(f"Cleaned up {len(po_ids)} existing po_ids.")

        # Append data (Table created automatically on first run)
        cleaned_df_po.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()
        print(f"Load complete for raw_purchase_orders. "
              f"{len(cleaned_df_po)} rows processed with insert_ts.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        conn.close()


def ingest_raw_shipments(cleaned_df_shipments):

    # Add the insert_ts column with current timestamp
    cleaned_df_shipments['insert_ts'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    table_name = "raw_shipments"
    conn = sqlite3.connect('supply_chain.db')
    cursor = conn.cursor()

    try:
        # Check if table exists
        cursor.execute(
            f"SELECT name FROM sqlite_master "
            f"WHERE type='table' AND name='{table_name}'"
        )
        if cursor.fetchone():
            # Delete existing records to prevent duplicates and allow updates
            shipment_ids = cleaned_df_shipments['shipment_id'].tolist()
            query = f"DELETE FROM {table_name} WHERE shipment_id IN ({','.join(['?'] * len(shipment_ids))})"
            cursor.execute(query, shipment_ids)
            print(f"Cleaned up {len(shipment_ids)} existing shipment_ids.")

        # Append data (Table created automatically on first run)
        cleaned_df_shipments.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()
        print(f"Load completed for raw_shipments. "
              f"{len(cleaned_df_shipments)} rows processed with insert_ts.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        conn.close()


def ingest_raw_inventory_snapshots(cleaned_df_inventory):

    incoming_snapshot = cleaned_df_inventory['snapshot_date'].max()

    table_name = "raw_inventory_snapshots"
    conn = sqlite3.connect("supply_chain.db")

    try:
        # 2. Get the current MAX snapshot from the database
        query = f"SELECT MAX(snapshot_date) FROM {table_name}"
        # If table doesn't exist, this will trigger the except block or
        # return None
        try:
            current_max_df = pd.read_sql(query, conn).iloc[0, 0]
        except:
            current_max_df = None

        # 3. Decision Logic: Only insert if incoming is strictly newer
        if current_max_df is None or incoming_snapshot > current_max_df:
            # Add your tracking timestamp
            cleaned_df_inventory['insert_ts'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cleaned_df_inventory.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"New snapshot {incoming_snapshot} loaded successfully.")
        else:
            print(
                f"Skip: Snapshot {incoming_snapshot} already exists or is "
                f"older than {current_max_df}.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
