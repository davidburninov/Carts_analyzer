import pandas as pd

from constants import *
from sqlalchemy import create_engine


PG_USER = 'user'
PG_PASSWORD = 'password'
PG_HOST = '00.000.000.000'
PG_PORT = 5432
PG_DB_NAME = 'ChargeCarts'

PG_ENGINE = create_engine(
    'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
        PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB_NAME
    )
)

_CART_TYPE_DICT = None


# Function returns data from database
def get_data(engine):
    sel_query = """
                SELECT *
                FROM public.log_data
                WHERE timestamp >= '%s'
                AND timestamp <= '%s'
                """ % (DEFAULT_TIMESTAMP_FROM, DEFAULT_TIMESTAMP_TO)
    result = pd.read_sql_query(
        sel_query,
        con=engine
    )
    result = result.sort_values(
        ['sensor', 'beacon', 'timestamp'],
        ascending=[True, True, True]
    )
    return result


# Function returns check data from database
def get_check_data(engine):
    sel_query = "SELECT timestamp, sensor_id AS sensor, with_charge FROM public.check_data_sensor"
    result = pd.read_sql_query(
        sel_query,
        con=engine)

    result = result.reset_index(drop=True)
    return result


# Function returns data for events in hall from database
def get_data_for_events_in_hall(engine):
    sel_query = """
                SELECT *
                FROM public.events_in_hall
                """
    result = pd.read_sql_query(
        sel_query,
        con=engine
    )
    return result


# Function returns dictionary of cart and type of cart from database
def _get_cart_type_dict():
    global PG_ENGINE

    tmp_table = pd.read_sql_query(
        'SELECT beacon_id, with_charge FROM public.beacon_address',
        con=PG_ENGINE
    )

    result = {}
    for _, row in tmp_table.iterrows():
        beacon_id, cart_type = row.tolist()
        result[beacon_id] = cart_type

    return result


# Function returns type of cart
def is_cart_with_charge(beacon_id):
    global _CART_TYPE_DICT

    if _CART_TYPE_DICT is None:
        _CART_TYPE_DICT = _get_cart_type_dict()

    return _CART_TYPE_DICT[beacon_id]


# I should replace this function
# Function returns lower and upper limit of number of cart with a given type
def get_beacon_bounds(with_charge, engine):
    sel_query = """
                SELECT MIN(beacon_id) as min,
                       MAX(beacon_id)+1 as max
                FROM public.beacon_address
                WHERE with_charge = '%i'
                """ % with_charge
    data = pd.read_sql_query(
        sel_query,
        con=engine
    )
    cycle_begin = data.loc[0, 'min']
    cycle_end = data.loc[0, 'max']

    return dict(
        [("begin", cycle_begin),
         ("end", cycle_end)]
    )
