import numpy as np
import matplotlib.pyplot as plt

from constants import *
from db_proxy import *
from events import *
from IPython.display import display, HTML


CART_PROPERTIES = dict(
    [("with_charge", ["blue", "Cart with charge"]),
     ("without_charge", ["red", "Cart without charge"]),
     ("check_with_charge", ["#007FFF", "Cart with charge from Check Table"]),
     ("check_without_charge", ["pink", "Cart without charge from Check Table"])]
)


# Function plot graph of events
def plot_carts(timestamp_data, data, sensor, sensor_init):
    array = data.loc[data['sensor'] == sensor].index.values

    cnt_shift = 0.125
    for i in xrange(array[0], array[len(array) - 1] + 1):
        df_var = pd.DataFrame()

        df_var['timestamp'] = pd.DataFrame(timestamp_data)
        df_var['sensor'] = np.nan
        df_var['with_charge'] = False

        df_var.loc[(
                       df_var['timestamp'] >= data.loc[i, 'timestamp_from']
                   ) & (
                       df_var['timestamp'] <= data.loc[i, 'timestamp_to']
                   ) & (
                       data.loc[i, 'sensor'] == sensor
                   ), 'sensor'] = data.loc[i, 'sensor'] + cnt_shift

        df_var.loc[(
                       df_var['timestamp'] >= data.loc[i, 'timestamp_from']
                   ) & (
                       df_var['timestamp'] <= data.loc[i, 'timestamp_to']
                   ) & (
                       data.loc[i, 'sensor'] == sensor
                   ), 'with_charge'] = data.loc[i, 'with_charge']

        if cnt_shift == 0.125:
            cnt_shift = -0.125
        else:
            cnt_shift = 0.125

        df_var = df_var.reset_index(drop=True)

        plt.plot(
            df_var[df_var.with_charge]['timestamp'],
            df_var[df_var.with_charge]['sensor'],
            lw=5,
            color=CART_PROPERTIES["with_charge"][0],
            label=CART_PROPERTIES["with_charge"][1] if (
                                                           i == array[0]
                                                       ) & (
                                                            sensor == sensor_init
                                                       ) else ""
        )

        plt.plot(
            df_var[df_var.with_charge == False]['timestamp'],
            df_var[df_var.with_charge == False]['sensor'],
            lw=5,
            color=CART_PROPERTIES["without_charge"][0],
            label=CART_PROPERTIES["without_charge"][1] if (
                                                              i == array[0]
                                                          ) & (
                                                              sensor == sensor_init
                                                          ) else ""
        )

        plt.ylim(0, 17)


# Function plot all event in all cashes
def plot_main_events(data, df_sel_events, check_data_sensor):
    plt.figure(figsize=(25, 15))

    data_short = data.loc[(
                              data['timestamp'] >= DEFAULT_TIMESTAMP_FROM
                          ) & (
                              data['timestamp'] <= DEFAULT_TIMESTAMP_TO
                          ), 'timestamp'].unique()

    horizontal_line = pd.DataFrame()
    horizontal_line['timestamp'] = pd.DataFrame(data_short)
    for i in xrange(1, 17):
        horizontal_line['sensor'] = i
        plt.plot(
            horizontal_line['timestamp'],
            horizontal_line['sensor'],
            lw=0.3,
            color='black',
            label=''
        )

    sensor_init = df_sel_events['sensor'].unique()[0]
    for sensor in df_sel_events['sensor'].unique():
        plot_carts(data_short, df_sel_events, sensor, sensor_init)

    plt.plot(
        check_data_sensor[check_data_sensor.with_charge]['timestamp'],
        check_data_sensor[check_data_sensor.with_charge]['sensor'],
        'ro',
        markersize=20,
        color=CART_PROPERTIES["check_with_charge"][0],
        label=CART_PROPERTIES["check_with_charge"][1]
    )

    plt.plot(
        check_data_sensor[check_data_sensor.with_charge == False]['timestamp'],
        check_data_sensor[check_data_sensor.with_charge == False]['sensor'],
        'ro',
        markersize=20,
        color=CART_PROPERTIES["check_without_charge"][0],
        label=CART_PROPERTIES["check_without_charge"][1]
    )

    plt.legend(bbox_to_anchor=(0, 1))
    plt.show()


def _mask_charge(data, with_charge):
    return data['with_charge'] == with_charge


# Function plot distribution time of carts
def plot_distribution_time():
    df_in_hall_grouped = get_data_for_events_in_hall(PG_ENGINE)

    df_in_hall_grouped_time = df_in_hall_grouped.loc[:, ['sensor', 'datediff', 'with_charge']]
    df_in_hall_grouped_time = df_in_hall_grouped_time.groupby(by=['with_charge', 'datediff'])['sensor'].count()
    df_in_hall_grouped_time = df_in_hall_grouped_time.reset_index()
    df_in_hall_grouped_time.loc[:, 'datediff'] = df_in_hall_grouped_time.loc[:, 'datediff'] / 60

    sum_by_charge = df_in_hall_grouped_time.groupby(by=['with_charge'])['sensor'].sum()

    df_in_hall_grouped_time['weight'] = 0

    for with_charge in [True, False]:
        mask = _mask_charge(df_in_hall_grouped_time, with_charge)
        df_in_hall_grouped_time.loc[mask, 'weight'] = df_in_hall_grouped_time.loc[mask, 'sensor'] * 1.0 / sum_by_charge[with_charge]
        df_in_hall_grouped_time.loc[mask, 'datediff'].hist(bins=np.arange(10, 200, 5),
                                                           weights=df_in_hall_grouped_time.loc[mask, 'weight'],
                                                           facecolor='blue')
        plt.title(with_charge)
        plt.show()

    # Median
    df_group_in_hall_med_time = df_in_hall_grouped.groupby('with_charge')['datediff'].median()
    display(df_group_in_hall_med_time)
