import pandas as pd


def preprocess_purchase_orders_data(df_po):
    # Create a copy to avoid modifying original
    df_po = df_po.copy()

    # Convert to datetime format
    df_po['order_date'] = pd.to_datetime(df_po['order_date'], format='mixed', dayfirst=True)
    df_po['delivery_date'] = pd.to_datetime(df_po['delivery_date'], format='mixed', dayfirst=True)

    df_po['is_delivery_date_given'] = df_po['delivery_date'].isna().map(
        {True: 'No', False: 'Yes'}
    )

    # Calculate median lead time per vendor
    df_po['lead_time'] = (df_po['delivery_date'] - df_po['order_date']).dt.days
    vendor_lead_times = df_po.groupby('vendor_id')['lead_time'].median()

    # Fill missing dates using vendor-specific lead times
    for vendor in vendor_lead_times.index:
        mask = (df_po['vendor_id'] == vendor) & (df_po['delivery_date'].isna())
        lead_time = vendor_lead_times[vendor]
        df_po.loc[mask, 'delivery_date'] = (df_po.loc[mask, 'order_date']
                                            + pd.Timedelta(days=lead_time))

    df_po['delivery_date'] = df_po['delivery_date'].dt.strftime('%Y-%m-%d')
    df_po['order_date'] = df_po['order_date'].dt.strftime('%Y-%m-%d')

    # Create flag based on null values before filling them
    df_po['is_po_qty_given'] = df_po['quantity'].isna().map(
        {True: 'No', False: 'Yes'}
    )

    # Fill null quantities with 0
    df_po['quantity'] = df_po['quantity'].fillna(0)

    # Remove entries with invalid vendor_id
    df_po = df_po[
        (df_po['vendor_id'].notna()) &
        (df_po['vendor_id'] != 'BAD') &
        (df_po['vendor_id'] != '') &
        (df_po['vendor_id'].str.startswith('V', na=False)) &
        (df_po['vendor_id'].str.len() == 4)
    ]

    return df_po


def preprocess_shipments_data(df_shipments):
    # Create a copy to avoid modifying original
    df_shipments = df_shipments.copy()

    # Convert to datetime format
    df_shipments['actual_delivery_date'] = pd.to_datetime(
        df_shipments['actual_delivery_date'],
        format='mixed',
        dayfirst=True
    )

    # Create flag based on null values before filling them
    df_shipments['is_shipments_qty_given'] = df_shipments[
        'shipment_quantity'
    ].isna().map(
        {True: 'No', False: 'Yes'}
    )

    # Fill null quantities with 0
    df_shipments['shipment_quantity'] = df_shipments['shipment_quantity'].fillna(0)

    return df_shipments


def preprocess_inventory_snapshots_data(df_inventory):
    df_inventory = df_inventory.copy()

    # Create flag based on null values before filling them
    df_inventory['is_current_stock_given'] = df_inventory[
        'current_stock'
    ].isna().map(
        {True: 'No', False: 'Yes'}
    )

    df_inventory['is_safety_stock_given'] = df_inventory[
        'safety_stock'
    ].isna().map(
        {True: 'No', False: 'Yes'}
    )

    # Fill null quantities with 0
    df_inventory['current_stock'] = df_inventory['current_stock'].fillna(0)
    df_inventory['safety_stock'] = df_inventory['safety_stock'].fillna(0)

    return df_inventory
