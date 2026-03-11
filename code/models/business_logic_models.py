import os

import pandas as pd
import sqlite3
from datetime import datetime


def build_supplier_performance_metrics():
    conn = sqlite3.connect("supply_chain.db")
    target_table = 'supplier_performance_metrics'
    query = """
    
        SELECT
            po.po_id,
            po.vendor_id,
            po.delivery_date AS planned_delivery_date,
            s.actual_delivery_date,
            -- Planned vs. actual delivery delta (in days)
            (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date)) AS delivery_delta,
            -- Delivery delay (only positive values, 0 if delivered early)
            CASE
                WHEN (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date)) > 0
                THEN (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date))
                ELSE 0
            END AS delivery_delay_days,
            -- On-time vs. late flag
            CASE
                WHEN s.actual_delivery_date <= po.delivery_date THEN 'On-Time'
                ELSE 'Late'
            END AS delivery_status
        FROM
            raw_purchase_orders po
        INNER JOIN
            raw_shipments s ON po.po_id = s.po_id
        ORDER BY
            po.vendor_id, po.po_id;
    
    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn)

        if df.empty:
            print("No data found to transfer.")
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['insert_ts'] = current_time

        df.to_sql(f'{target_table}', conn, if_exists='append', index=False)

        print(
            f"Successfully inserted {len(df)} rows into '{target_table}' at {current_time}.")

    except Exception as e:
        print(f"Error during transfer: {e}")
    finally:
        conn.close()


def build_aggregated_vendor_kpis():
    conn = sqlite3.connect("supply_chain.db")
    target_table = 'aggregated_vendor_kpis'
    query = """

        WITH latest_supplier_performance_data AS (
            SELECT 
                * 
            FROM 
                supplier_performance_metrics 
            WHERE insert_ts = (
                SELECT MAX(insert_ts) FROM supplier_performance_metrics
            )
        )
        
        SELECT
            vendor_id,
            COUNT(po_id) AS total_orders,
            -- On-Time Delivery Performance
            ROUND(
                AVG(
                    CASE
                        WHEN actual_delivery_date <= planned_delivery_date THEN 100.0
                        ELSE 0
                    END
                ), 2
            ) AS on_time_delivery_percentage,
            -- Average Delay Statistics
            ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
            -- Delivery Status Breakdown
            SUM(CASE WHEN delivery_status = 'On-Time' THEN 1 ELSE 0 END) AS on_time_deliveries,
            SUM(CASE WHEN delivery_status = 'Late' THEN 1 ELSE 0 END) AS late_deliveries,
            -- Maximum Delay
            MAX(delivery_delay_days) AS max_delay_days
        FROM
            latest_supplier_performance_data
        GROUP BY
            vendor_id
        ORDER BY
            on_time_delivery_percentage DESC;
    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn)

        if df.empty:
            print("No data found to transfer.")
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['insert_ts'] = current_time

        df.to_sql(f'{target_table}', conn, if_exists='append', index=False)

        print(
            f"Successfully inserted {len(df)} rows into '{target_table}' at {current_time}.")

    except Exception as e:
        print(f"Error during transfer: {e}")
    finally:
        conn.close()


def build_material_inventory_risk():
    conn = sqlite3.connect("supply_chain.db")
    target_table = 'material_inventory_risk'
    query = """

        WITH STOCK_BELOW_SAFETY AS (
            SELECT
                material_id,
                -- Calculate frequency of stock below safety stock
                COUNT(*) AS total_snapshots,
                SUM(
                    CASE
                        WHEN current_stock < safety_stock THEN 1
                        ELSE 0
                    END
                ) AS times_below_safety,
                -- Get most recent snapshot
                MAX(snapshot_date) AS latest_snapshot_date
            FROM
                raw_inventory_snapshots
            GROUP BY
                material_id
        )
        
        , LATEST_INV_STATUS AS (
            SELECT
                i.material_id,
                i.current_stock,
                i.safety_stock,
                i.snapshot_date,
                (i.current_stock - i.safety_stock) AS stock_margin
            FROM
                raw_inventory_snapshots i
            INNER JOIN
                STOCK_BELOW_SAFETY s ON i.material_id = s.material_id
                AND i.snapshot_date = s.latest_snapshot_date
        )
        
        SELECT
            sbs.material_id,
            -- Frequency calculation
            ROUND((sbs.times_below_safety * 100.0 / sbs.total_snapshots), 2) AS pct_time_below_safety,
            -- Current inventory status
            CASE
                WHEN ls.current_stock < ls.safety_stock THEN 'Below Safety Stock'
                WHEN ls.current_stock < (ls.safety_stock * 1.2) THEN 'Near Safety Stock'
                ELSE 'Adequate Stock'
            END AS current_inventory_status,
            -- Risk classification
            CASE
                WHEN (sbs.times_below_safety * 100.0 / sbs.total_snapshots) >= 20 THEN 'High Risk'
                WHEN (sbs.times_below_safety * 100.0 / sbs.total_snapshots) >= 10 THEN 'Medium Risk'
                ELSE 'Low Risk'
            END AS risk_classification,
            -- Additional context
            ls.current_stock,
            ls.safety_stock,
            ls.stock_margin,
            ls.snapshot_date AS last_updated
        FROM
            STOCK_BELOW_SAFETY sbs
        JOIN
            LATEST_INV_STATUS ls ON sbs.material_id = ls.material_id
        ORDER BY
            pct_time_below_safety DESC;

    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn)

        if df.empty:
            print("No data found to transfer.")
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['insert_ts'] = current_time

        df.to_sql(f'{target_table}', conn, if_exists='append', index=False)

        print(
            f"Successfully inserted {len(df)} rows into '{target_table}' at {current_time}.")

    except Exception as e:
        print(f"Error during transfer: {e}")
    finally:
        conn.close()


def build_supply_chain_health_data():
    conn = sqlite3.connect("supply_chain.db")
    target_table = 'supply_chain_health'
    query = """
        WITH LATEST_VENDOR_KPIS AS (
            SELECT
                vendor_id,
                total_orders,
                on_time_delivery_percentage,
                avg_delay_days
            FROM aggregated_vendor_kpis
            WHERE insert_ts = (
                SELECT MAX(insert_ts) FROM aggregated_vendor_kpis
            )
        )
        
        , LATEST_INV_RISK AS (
            SELECT
                material_id,
                pct_time_below_safety,
                current_stock,
                safety_stock,
                current_inventory_status,
                risk_classification
            FROM material_inventory_risk
            WHERE insert_ts = (
                SELECT MAX(insert_ts) FROM material_inventory_risk
            )
        )
               
        SELECT
            po.po_id,
            po.vendor_id,
            po.material_id,
            s.shipment_id,
            po.quantity,
            po.order_date,
            po.delivery_date AS planned_delivery_date,
            s.actual_delivery_date,
            (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date)) AS delivery_delta,
            CASE
                WHEN (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date)) > 0
                THEN (JULIANDAY(s.actual_delivery_date) - JULIANDAY(po.delivery_date))
                ELSE 0
            END AS delivery_delay_days,
            CASE
                WHEN s.actual_delivery_date <= po.delivery_date THEN 'On-Time'
                ELSE 'Late'
            END AS delivery_status,
            vk.total_orders AS vendor_total_orders,
            vk.on_time_delivery_percentage AS vendor_otd_percentage,
            vk.avg_delay_days AS vendor_avg_delay_days,
            ir.current_stock,
            ir.safety_stock,
            ir.pct_time_below_safety,
            ir.current_inventory_status AS current_inventory_status,
            ir.risk_classification as inventory_risk_classification
        FROM
            raw_purchase_orders po
        INNER JOIN
            raw_shipments s ON po.po_id = s.po_id
        INNER JOIN
            LATEST_VENDOR_KPIS vk ON po.vendor_id = vk.vendor_id
        INNER JOIN
            LATEST_INV_RISK ir ON po.material_id = ir.material_id
        ORDER BY
            po.delivery_date DESC;
    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn)

        if df.empty:
            print("No data found to transfer.")
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['insert_ts'] = current_time

        script_dir = os.path.dirname(os.path.abspath(__file__))
        target_dir = os.path.abspath(os.path.join(script_dir, "..", "..",
                                                  "data", "analytics"))
        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, "supply_chain_health.csv")
        df.to_csv(file_path, index=False)
        file_path = os.path.join(target_dir, "dashboard_ready.csv")
        df.to_csv(file_path, index=False)

        df.to_sql(f'{target_table}', conn, if_exists='append', index=False)

        print(
            f"Successfully inserted {len(df)} rows into '{target_table}' at {current_time}.")

    except Exception as e:
        print(f"Error during transfer: {e}")
    finally:
        conn.close()
