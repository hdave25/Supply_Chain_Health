from code.data_preprocessing.data_preprocessing import *
from code.ingestions.raw_ingestions import *
from code.models.business_logic_models import *


# Main driver file for data preprocessing, ingestion and business models


def preprocess_data():
    # Read raw csv files
    df_po = pd.read_csv('data/raw_data/purchase_orders.csv')
    df_shipments = pd.read_csv('data/raw_data/shipments.csv')
    df_inventory = pd.read_csv('data/raw_data/inventory_snapshots.csv')

    cleaned_df_po = preprocess_purchase_orders_data(df_po)
    cleaned_df_po.to_csv('data/preprocessed_data/cleaned_purchase_orders.csv',
                         index=False)
    cleaned_df_shipments = preprocess_shipments_data(df_shipments)
    cleaned_df_shipments.to_csv('data/preprocessed_data/cleaned_shipments.csv',
                                index=False)
    cleaned_df_inventory = preprocess_inventory_snapshots_data(df_inventory)
    cleaned_df_inventory.to_csv('data/preprocessed_data'
                                '/cleaned_inventory_snapshots.csv',
                                index=False)


def ingest_raw_data():
    # Ingest raw data for purchase_orders, shipments and inventory_snapshots

    # Read raw data from cleaned csv files
    cleaned_df_po = pd.read_csv('data/preprocessed_data'
                                '/cleaned_purchase_orders.csv')
    cleaned_df_shipments = pd.read_csv('data/preprocessed_data'
                                       '/cleaned_shipments.csv')
    cleaned_df_inventory = pd.read_csv('data/preprocessed_data'
                                       '/cleaned_inventory_snapshots.csv')

    ingest_raw_purchase_orders(cleaned_df_po)
    ingest_raw_shipments(cleaned_df_shipments)
    ingest_raw_inventory_snapshots(cleaned_df_inventory)


def run_business_logic_models():
    build_supplier_performance_metrics()
    build_aggregated_vendor_kpis()
    build_material_inventory_risk()
    build_supply_chain_health_data()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Preprocessing of data
    preprocess_data()

    # Ingestion of cleaned data to raw (bronze) layer
    ingest_raw_data()

    # Business logic models (silver & gold layer models)
    run_business_logic_models()
