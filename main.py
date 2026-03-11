from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import os
import pandas as pd
import sqlite3

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get('/', response_class=HTMLResponse)
def root():
    return """
    
        <html>
        <head>
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; 
                       display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #fdfdfd; }
                .container { text-align: center; border: 1px solid #eaeaea; padding: 40px; border-radius: 8px; background: white; }
                h1 { color: #333; margin-bottom: 10px; }
                p { color: #666; margin-bottom: 20px; }
                .btn { text-decoration: none; background: #007bff; color: white; padding: 10px 20px; border-radius: 5px; font-weight: 500; }
                .btn:hover { background: #0056b3; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📦 Welcome to Supply Chain Health App 📦</h1>
                <p>
                    Go to /docs/api_usage.md to explore more!!!
                    <br> Or read about API Usage in the repository.
                </p>
                <a href="https://github.com/hdave25/Supply_Chain_Health/blob/main/docs/api_usage.md" class="btn" target="_blank">Explore API Docs</a>
            </div>
        </body>
    </html>
    
    """


@app.get("/data_processed/cleaned_inventory_snapshots.csv")
async def get_cleaned_inventory_snapshots_data():
    file_path = "data/preprocessed_data/cleaned_inventory_snapshots.csv"

    if not os.path.exists(file_path):
        return "File not found. Please run the cleaning script first."

    return FileResponse(
        path=file_path,
        filename="cleaned_inventory_snapshots.csv",
        media_type="text/csv"
    )


@app.get("/data_processed/cleaned_purchase_orders.csv")
async def get_cleaned_purchase_orders_data():
    file_path = "data/preprocessed_data/cleaned_purchase_orders.csv"

    if not os.path.exists(file_path):
        return "File not found. Please run the cleaning script first."

    return FileResponse(
        path=file_path,
        filename="cleaned_purchase_orders.csv",
        media_type="text/csv"
    )


@app.get("/data_processed/cleaned_shipments.csv")
async def get_cleaned_shipments_data():
    file_path = "data/preprocessed_data/cleaned_shipments.csv"

    if not os.path.exists(file_path):
        return "File not found. Please run the cleaning script first."

    return FileResponse(
        path=file_path,
        filename="cleaned_shipments.csv",
        media_type="text/csv"
    )


@app.get("/analytics/supply_chain_health.csv")
async def get_supply_chain_health_data():
    file_path = "data/analytics/supply_chain_health.csv"

    if not os.path.exists(file_path):
        return "File not found. Please run the cleaning script first."

    return FileResponse(
        path=file_path,
        filename="supply_chain_health.csv",
        media_type="text/csv"
    )


@app.get("/docs/data_preprocessing.md")
async def get_data_preprocessing():
    file_path = "docs/data_preprocessing.md"

    if not os.path.exists(file_path):
        return "File not found. Please run the cleaning script first."

    return FileResponse(
        path=file_path,
        filename="data_preprocessing.md",
        media_type="text/markdown"
    )


@app.get("/docs/business_logic.md")
async def get_business_logic():
    file_path = "docs/business_logic.md"

    if not os.path.exists(file_path):
        return "File not found. Please run the cleaning script first."

    return FileResponse(
        path=file_path,
        filename="business_logic.md",
        media_type="text/markdown"
    )


@app.get("/supplier_performance_metrics", response_class=HTMLResponse)
async def get_supplier_performance(request: Request):
    conn = sqlite3.connect("supply_chain.db")

    query = """
    
        SELECT 
            *
        FROM supplier_performance_metrics
        WHERE insert_ts = (
            SELECT MAX(insert_ts) FROM supplier_performance_metrics
        )
    
    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn)

        if df.empty:
            print("No data found to transfer.")
            return

        dfs = {
            f"Supplier Performance Metrics": df,
        }

        html_tables = {
            name: df.to_html(classes="table table-hover table-bordered",
                             index=False)
            for name, df in dfs.items()
        }

        return templates.TemplateResponse(
            "report.html",
            {"request": request, "tables": html_tables}
        )

    except Exception as e:
        print(f"Error during load: {e}")
    finally:
        conn.close()


@app.get("/vendor_performance", response_class=HTMLResponse)
async def get_vendor_performance(vendor_id: str, request: Request):
    conn = sqlite3.connect("supply_chain.db")
    query = """

        SELECT 
            *
        FROM aggregated_vendor_kpis
        WHERE insert_ts = (
            SELECT MAX(insert_ts) FROM aggregated_vendor_kpis
        )
        AND vendor_id = ?

    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn, params=[vendor_id])

        if df.empty:
            print("No data found to transfer.")
            return

        dfs = {
            f"Vendor Performance for {df['vendor_id'].iloc[0]}": df,
        }

        html_tables = {
            name: df.to_html(classes="table table-hover table-bordered",
                             index=False)
            for name, df in dfs.items()
        }

        return templates.TemplateResponse(
            "report.html",
            {"request": request, "tables": html_tables, "title": "Vendor Performance"}
        )

    except Exception as e:
        print(f"Error during load: {e}")
    finally:
        conn.close()


@app.get("/material_risk", response_class=HTMLResponse)
async def get_material_risk(material_id: str, request: Request):
    conn = sqlite3.connect("supply_chain.db")
    query = """

        SELECT 
            *
        FROM material_inventory_risk
        WHERE insert_ts = (
            SELECT MAX(insert_ts) FROM material_inventory_risk
        )
        AND material_id = ?

    """

    try:
        print("Fetching data from source...")
        df = pd.read_sql(query, conn, params=[material_id])

        if df.empty:
            print("No data found to transfer.")
            return

        dfs = {
            f"Material Risk for {df['material_id'].iloc[0]}": df,
        }

        html_tables = {
            name: df.to_html(classes="table table-hover table-bordered",
                             index=False)
            for name, df in dfs.items()
        }

        return templates.TemplateResponse(
            "report.html",
            {"request": request, "tables": html_tables, "title": "Material Risk"}
        )

    except Exception as e:
        print(f"Error during load: {e}")
    finally:
        conn.close()


@app.get("/health_summary", response_class=HTMLResponse)
async def get_health_summary(request: Request):
    conn = sqlite3.connect("supply_chain.db")
    query_1 = """
        
        SELECT 
            risk_classification,
            COUNT(*) AS number_of_materials,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM material_inventory_risk
        WHERE insert_ts = (
            SELECT MAX(insert_ts) FROM material_inventory_risk
        )
        GROUP BY risk_classification;

    """

    query_2 = """
        
        SELECT 
            vendor_id,
            on_time_delivery_percentage
        FROM aggregated_vendor_kpis
        WHERE insert_ts = (
            SELECT MAX(insert_ts) FROM aggregated_vendor_kpis
        )
        ORDER BY on_time_delivery_percentage DESC
        LIMIT 5;
            
    """

    query_3 = """

        SELECT 
            delivery_status,
            COUNT(*) AS number_of_purchase_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM supplier_performance_metrics
        WHERE insert_ts = (
            SELECT MAX(insert_ts) FROM supplier_performance_metrics
        )
        GROUP BY delivery_status;

    """

    try:
        print("Fetching data from source...")
        df_1 = pd.read_sql(query_1, conn)
        df_2 = pd.read_sql(query_2, conn)
        df_3 = pd.read_sql(query_3, conn)

        if df_1.empty and df_2.empty and df_3.empty:
            print("No data found to transfer.")
            return

        dfs = {
            "Material Inventory Health": df_1,
            "Aggregated Vendor Performance": df_2,
            "Supplier Performance Metrics": df_3
        }

        html_tables = {
            name: df.to_html(classes="table table-hover table-bordered",
                             index=False)
            for name, df in dfs.items()
        }

        return templates.TemplateResponse(
            "report.html",
            {"request": request, "tables": html_tables, "title": "Health Summary"}
        )

    except Exception as e:
        print(f"Error during load: {e}")
    finally:
        conn.close()
