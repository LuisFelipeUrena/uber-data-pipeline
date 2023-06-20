import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    df = data
    # reset date columns to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # dropping duplicates, resseting indexes and creating column trip_id
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    # dimensional table dropoff dates

    dim_dropoff_dates  = df[['tpep_dropoff_datetime']].reset_index(drop=True)

    dim_dropoff_dates['dropoff_hour'] = dim_dropoff_dates['tpep_dropoff_datetime'].dt.hour
    dim_dropoff_dates['dropoff_day'] = dim_dropoff_dates['tpep_dropoff_datetime'].dt.day
    dim_dropoff_dates['dropoff_month'] = dim_dropoff_dates['tpep_dropoff_datetime'].dt.month
    dim_dropoff_dates['dropoff_year'] = dim_dropoff_dates['tpep_dropoff_datetime'].dt.year
    dim_dropoff_dates['dropoff_weekday'] = dim_dropoff_dates['tpep_dropoff_datetime'].dt.weekday

    dim_dropoff_dates['do_date_id'] = dim_dropoff_dates.index

    # dimensional table: dim_pickup_dates

    dim_pickup_dates  = df[['tpep_pickup_datetime']].reset_index(drop=True)

    dim_pickup_dates['pickup_hour'] = dim_pickup_dates['tpep_pickup_datetime'].dt.hour
    dim_pickup_dates['pickup_day'] = dim_pickup_dates['tpep_pickup_datetime'].dt.day
    dim_pickup_dates['pickup_month'] = dim_pickup_dates['tpep_pickup_datetime'].dt.month
    dim_pickup_dates['pickup_year'] = dim_pickup_dates['tpep_pickup_datetime'].dt.year
    dim_pickup_dates['pickup_weekday'] = dim_pickup_dates['tpep_pickup_datetime'].dt.weekday

    dim_pickup_dates['pu_date_id'] = dim_pickup_dates.index

    # Dimensional table: dim trip_distance

    dim_trip_distance = df[['trip_distance']].reset_index(drop=True)

    dim_trip_distance.rename(columns={'trip_distance':'distance_miles'}, inplace=True)

    dim_trip_distance['trip_distance_id'] = dim_trip_distance.index

    dim_trip_distance = dim_trip_distance[['trip_distance_id','distance_miles']]
    
    # Dimensional table: dim_coordinates

    dim_coordinates = df[['pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude']].reset_index(drop=True)

    new_col_names = {
                     'pickup_longitude':'pickup_long',
                     'pickup_latitude':'pickup_lat',
                     'dropoff_longitude':'dropoff_long',
                     'dropoff_latitude':'dropoff_lat',
                     }
    dim_coordinates.rename(columns=new_col_names,inplace=True)

    dim_coordinates['coord_id'] = dim_coordinates.index
    dim_coordinates = dim_coordinates[['coord_id','pickup_long', 'pickup_lat','dropoff_long','dropoff_lat']]

    # Dimensional table: dim_payment_type
    dim_payments = df[['payment_type']].reset_index(drop=True)

    payment_type_dict = {
        1:'Credit card',
        2:'Cash',
        3:'No charge',
        4:'Dispute',
        5:'Unknown',
        6:'Voided trip',
    }

    dim_payments['payment_type_text'] = dim_payments['payment_type'].map(payment_type_dict)

    dim_payments['pt_id'] = dim_payments.index

    dim_payments = dim_payments[['pt_id','payment_type','payment_type_text']]


    # Dimensional table: dim_ratecode
    dim_ratecode = df[['RatecodeID']].reset_index(drop=True)

    rc_dict = {
        1:'Standard rate',
        2:'JFK',
        3:'Newark',
        4:'Nassau or Westchester',
        5:'Negotiated fare',
        6:'Group ride',
    }

    dim_ratecode['ratecode_value'] = dim_ratecode['RatecodeID'].map(rc_dict)

    dim_ratecode['rc_id'] = dim_ratecode.index

    dim_ratecode = dim_ratecode[['rc_id','ratecode_value','RatecodeID']]


    # Dimensional table: Passenger count
    dim_passengers = df[['passenger_count']].reset_index(drop=True)

    dim_passengers['pc_id'] = dim_passengers.index

    dim_passengers = dim_passengers[['pc_id', 'passenger_count']]

    # fact table:

    fct_table = df.merge(dim_pickup_dates, left_on='trip_id', right_on='pu_date_id')\
              .merge(dim_dropoff_dates, left_on='trip_id',right_on='do_date_id')\
              .merge(dim_ratecode,left_on='trip_id', right_on='rc_id')\
              .merge(dim_trip_distance, left_on='trip_id', right_on='trip_distance_id')\
              .merge(dim_coordinates,left_on='trip_id',right_on='coord_id')\
              .merge(dim_passengers, left_on='trip_id', right_on='pc_id')\
              .merge(dim_payments,left_on='trip_id', right_on='pt_id')\
              [['trip_id','VendorID','pu_date_id','do_date_id','rc_id','coord_id','trip_distance_id',
                'pc_id','pt_id','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
                'total_amount']]

    return {
        "dim_payments":dim_payments.to_dict(orient='dict'),
        "dim_passengers":dim_passengers.to_dict(orient='dict'),
        "dim_coordinates":dim_coordinates.to_dict(orient='dict'),
        "dim_trip_distance":dim_trip_distance.to_dict(orient='dict'),
        "dim_ratecode":dim_ratecode.to_dict(orient='dict'),
        "dim_dropoff_dates":dim_dropoff_dates.to_dict(orient='dict'),
        "dim_pickup_dates":dim_pickup_dates.to_dict(orient='dict'),
        "fct_table":fct_table.to_dict(orient='dict'),
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
