import matplotlib.pyplot as plt

from constants import *
from db_proxy import *


# Function plot graph of data
def plot_sensor_data(data, sensor, with_charge, with_limit):
    plt.figure(figsize=(25, 15))
    cnt = 0
    bounds = get_beacon_bounds(with_charge, PG_ENGINE)
    for j in range(bounds['begin'], bounds['end']):
        data_every = data.loc[
            (data['sensor'] == sensor) & (data['beacon'] == j) &
            (data['timestamp'] >= DEFAULT_TIMESTAMP_FROM) &
            (data['timestamp'] <= DEFAULT_TIMESTAMP_TO)
        ]
        if not data_every.empty:
            data_every['limit'] = EVENT_MIN_RSSI_LEVEL

            if with_limit & (cnt == 0):
                plt.plot(data_every['timestamp'], data_every['limit'], lw=3, label='Border', color='black')
                plt.legend(bbox_to_anchor=(0, 1))
                plt.ylim(-105, -35)

            plt.plot(data_every['timestamp'], data_every['rssi'], lw=1.5, label='s ' + str(sensor) + ', b ' + str(j))
            plt.legend(bbox_to_anchor=(0, 1))
            plt.ylim(-105, -35)

            cnt += 1
