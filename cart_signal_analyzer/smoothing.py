import numpy as np

DEFAULT_TRANSFORM_INTERPOLATE_LIMIT_SEC = 30
DEFAULT_TRANSFORM_EWM_LEVEL = 2
DEFAULT_TRANSFORM_EWM_SPAN_SEC = 30
DEFAULT_TRANSFORM_EWM_MIN_PERIODS_SEC = 3


def _adjust_value(value, multiplier, min_bound=1):
    value //= multiplier
    return max(min_bound, value)


# Function returns transformed data
def transform_data(data, for_plot=False, **kwargs):
    interpolate_limit = kwargs.get(
        'interpolate_limit_sec',
        DEFAULT_TRANSFORM_INTERPOLATE_LIMIT_SEC
    )
    interpolate_limit = _adjust_value(interpolate_limit, 3)

    ewm_level = kwargs.get(
        'ewm_level',
        DEFAULT_TRANSFORM_EWM_LEVEL
    )
    ewm_span = kwargs.get(
        'ewm_span_sec',
        DEFAULT_TRANSFORM_EWM_SPAN_SEC
    )
    ewm_span = _adjust_value(ewm_span, 3)

    ewm_min_periods = kwargs.get(
        'ewm_min_periods_sec',
        DEFAULT_TRANSFORM_EWM_MIN_PERIODS_SEC
    )
    ewm_min_periods = _adjust_value(ewm_min_periods, 3)
    ewm_ignore_na = kwargs.get('ewm_ignore_na', False)

    data = data.set_index(
        'timestamp',
        drop=True
    )
    data = data.groupby(
        ['sensor', 'beacon']
    ).resample('3S').rssi.mean()

    data = data.interpolate(
        method='linear',
        limit=interpolate_limit
    )

    na_mask = data.isnull()

    for i in xrange(ewm_level):
        data = data.ewm(
            span=ewm_span,
            min_periods=ewm_min_periods,
            ignore_na=ewm_ignore_na
        ).mean()
        if i != 0:
            data[na_mask] = np.nan

    data = data.reset_index().set_index(
        'timestamp',
        drop=True
    )
    data = data.groupby(
        ['sensor', 'beacon']
    ).resample('1S').rssi.mean()

    if for_plot:
        data = data.interpolate(method='linear', limit=60)
    else:
        data = data.interpolate(method='linear')

    return data.reset_index()
