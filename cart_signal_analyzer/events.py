import datetime

from constants import *
from db_proxy import *


# Function returns the capacity of event
def square_of_hat(data, event_min_rssi_level):
    data_rssi = (data.loc[:, 'rssi'] - event_min_rssi_level) ** 4
    return data_rssi.sum()


# Function return list of all events in format
# (sensor number, beacon number, start time of event, end time of event)
def find_all_events(preprocessed_log_data, event_min_rssi_level, events_min_timedelta_sec):
    events_timestamps = []

    level_mask = preprocessed_log_data['rssi'] > event_min_rssi_level

    preprocessed_log_data = preprocessed_log_data.loc[level_mask]

    sensor_mask = preprocessed_log_data['sensor'] < 18
    preprocessed_log_data = preprocessed_log_data.loc[sensor_mask].reset_index(drop=True)

    events_min_timedelta = datetime.timedelta(
        seconds=events_min_timedelta_sec
    )
    for group_name, group in preprocessed_log_data.groupby(('sensor', 'beacon')):
        sensor, beacon = group_name

        group_timestamps = group['timestamp'].sort_values()

        start_timestamp = group_timestamps.iloc[0]
        current_timestamp = group_timestamps.iloc[0]
        for i in xrange(1, len(group_timestamps) - 1):
            if group_timestamps.iloc[i] - current_timestamp > events_min_timedelta:
                prev_timestamp = current_timestamp
                events_timestamps.append({
                    'sensor': sensor,
                    'beacon': beacon,
                    'timestamp_from': start_timestamp,
                    'timestamp_to': prev_timestamp
                })
                start_timestamp = group_timestamps.iloc[i]

            current_timestamp = group_timestamps.iloc[i]

        prev_timestamp = group_timestamps.iloc[len(group_timestamps) - 1]
        events_timestamps.append({
            'sensor': sensor,
            'beacon': beacon,
            'timestamp_from': start_timestamp,
            'timestamp_to': prev_timestamp
        })

    events = []
    for i in xrange(len(events_timestamps)):
        if (
            events_timestamps[i]['timestamp_to'] - events_timestamps[i]['timestamp_from']
        ).seconds >= MIN_DURATION_EVENT_IN_CASH_SEC:
            events.append(events_timestamps[i])

    return events


# Function calculate the capacity of event
def calc_capacity(data, event, event_min_rssi_level):
    data_every = data.loc[(data['sensor'] == event['sensor'])
                          & (data['beacon'] == event['beacon'])
                          & (data['timestamp'] >= event['timestamp_from'])
                          & (data['timestamp'] <= event['timestamp_to'])]

    if not data_every.empty:
        event['capacity'] = square_of_hat(data_every, event_min_rssi_level)


# Function returns list of all events in format
# (sensor number, beacon number, start time of event, end time of event, capacity of event)
def find_list_events(data, event_min_rssi_level, timedelta):
    data_short = data.loc[data['rssi'] >= event_min_rssi_level]
    events = find_all_events(data_short, event_min_rssi_level, timedelta)
    for i in xrange(len(events)):
        calc_capacity(data_short, events[i], event_min_rssi_level)

    return events


# Function returns the events taking place at this time
def get_events_spec_time(data, sensor, beacon, t_from, t_to, timedelta):
    delta = datetime.timedelta(0, timedelta)

    mask_left_intersec_segment = (data['timestamp_from'] <= t_to + delta) & (data['timestamp_to'] >= t_to)
    mask_right_intersec_segment = (data['timestamp_from'] <= t_from) & (data['timestamp_to'] >= t_from - delta)
    mask_out = (data['timestamp_from'] >= t_from) & (data['timestamp_to'] <= t_to)
    mask_in = (data['timestamp_from'] <= t_from) & (data['timestamp_to'] >= t_to)

    common_mask = mask_left_intersec_segment | mask_right_intersec_segment | mask_out | mask_in
    mask_beacon = (data['beacon'] == beacon)
    mask_sensor = (data['sensor'] == sensor) & (data['beacon'] != beacon)

    data_spec_time = data.loc[common_mask & mask_beacon]

    return data_spec_time


def _get_ratio_numbers(a, b):
    return (a - b) * 1.0 / a * 100


# Function returns unique events with max capacity taking place at this time
def get_correct_events(data, timedelta):
    list_correct_events = []
    i = 0
    while i < len(data):
        data_row_cur = data.loc[i]
        df_event = get_events_spec_time(data,
                                        data_row_cur['sensor'],
                                        data_row_cur['beacon'],
                                        data_row_cur['timestamp_from'],
                                        data_row_cur['timestamp_to'],
                                        timedelta)

        if len(df_event) == 1:
            if data_row_cur['sensor'] >= 17:
                list_correct_events.append(dict(data_row_cur))
            i += 1

        else:
            cnt_index_less = (i > pd.Series(df_event.index.values)).sum()

            if df_event['capacity'].max() >= data['capacity'][i]:

                df_event = df_event.sort_values(['capacity'], ascending=[False]).reset_index(drop=True)
                row = dict(df_event.loc[0])

                if (
                    _get_ratio_numbers(df_event.loc[0, 'capacity'],
                                       df_event.loc[1, 'capacity']) >= MIN_DELTA_CAPACITY_PERCENT
                ) & (
                    row not in list_correct_events
                ):
                    list_correct_events.append(row)

            i = i + len(df_event) - cnt_index_less
    return pd.DataFrame(list_correct_events)


# Function returns the final events
def get_events(data, event_min_rssi_level, timedelta):
    list_events = find_list_events(data, event_min_rssi_level, 6 * timedelta)

    df_all_events = pd.DataFrame(list_events)
    df_all_events = df_all_events.sort_values(
        ['beacon', 'timestamp_from', 'timestamp_to', 'sensor'],
        ascending=[True, True, True, True]
    )
    df_all_events = df_all_events[['beacon', 'sensor', 'timestamp_from', 'timestamp_to', 'capacity']].reset_index(
        drop=True
    )

    df_sel_events = get_correct_events(df_all_events, timedelta)

    df_sel_events = df_sel_events.sort_values(
        ['sensor', 'timestamp_from', 'timestamp_to', 'beacon'],
        ascending=[True, True, True, True]
    )
    df_sel_events = df_sel_events[['sensor', 'beacon', 'timestamp_from', 'timestamp_to', 'capacity']].reset_index(drop=True)

    for i in xrange(len(df_sel_events)):
        df_sel_events.loc[i, 'with_charge'] = is_cart_with_charge(df_sel_events.loc[i, 'beacon'])

    return df_sel_events


# Function return table that describe how much time cart spent in hall
def events_in_hall(data_parking, data_cash, timedelta_hall_bound_sec):
    array_parking = data_parking.index.values
    array_mag = data_cash.index.values
    in_hall = []

    for i in array_parking:
        beacon_cur_park = data_parking.loc[i, 'beacon']
        timestamp_from_cur = data_parking.loc[i, 'timestamp_to']
        with_charge_cur = data_parking.loc[i, 'with_charge']

        for j in array_mag:
            timestamp_to_cur = data_cash.loc[j, 'timestamp_from']
            sensor_cur = data_cash.loc[j, 'sensor']
            beacon_cur_sel = data_cash.loc[j, 'beacon']

            if (
                    (
                        beacon_cur_park == beacon_cur_sel
                    ) & (
                        timestamp_from_cur <= timestamp_to_cur
                    ) & (
                        (timestamp_to_cur - timestamp_from_cur).seconds >= timedelta_hall_bound_sec
                    )
            ):
                in_hall.append({
                    "sensor": sensor_cur,
                    "beacon": beacon_cur_park,
                    "timestamp_from": timestamp_from_cur,
                    "timestamp_to": timestamp_to_cur,
                    "with_charge": with_charge_cur
                })
                break

    data_in_hall = pd.DataFrame(in_hall, columns=['sensor', 'beacon', 'timestamp_from', 'timestamp_to', 'with_charge'])

    data_in_hall_grouped = data_in_hall.groupby(by=['timestamp_to', 'sensor', 'beacon', 'with_charge']).max()
    data_in_hall_grouped = data_in_hall_grouped.reset_index(drop=False)

    return data_in_hall_grouped


# Function write data events in hall to database
def add_events_in_hall(data):
    df_sel_events = get_events(data, EVENT_MIN_RSSI_LEVEL, TIMEDELTA_SEC)
    df_sel_events = df_sel_events.loc[df_sel_events['sensor'] < 17]
    df_sel_events = df_sel_events.sort_values(
        ['beacon', 'timestamp_from', 'timestamp_to'],
        ascending=[True, True, False]
    )

    df_for_parking = get_events(data, EVENT_MIN_RSSI_LEVEL_FOR_PARKING, TIMEDELTA_SEC)
    df_for_parking = df_for_parking.loc[df_for_parking['sensor'] == 17].sort_values(
        ['beacon', 'timestamp_from', 'timestamp_to'],
        ascending=[True, True, False]
    )
    df_for_parking = df_for_parking.reset_index(drop=True)

    df_in_hall_grouped = events_in_hall(df_for_parking, df_sel_events, TIMEDELTA_HALL_BOUND_SEC)

    array_hall_grouped = df_in_hall_grouped.index.values
    df_in_hall_grouped['datediff'] = 0

    for i in array_hall_grouped:
        timedelta_in_hall = df_in_hall_grouped.loc[i, 'timestamp_to'] - df_in_hall_grouped.loc[i, 'timestamp_from']
        df_in_hall_grouped.loc[i, 'datediff'] = timedelta_in_hall.seconds

    df_in_hall_grouped.loc[:,['sensor', 'beacon','timestamp_from', 'timestamp_to','with_charge', 'datediff']].to_sql(
        'events_in_hall',
        PG_ENGINE,
        if_exists='append',
        index=False
    )
