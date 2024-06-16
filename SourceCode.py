import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import functools as ft


cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)


url = "https://archive-api.open-meteo.com/v1/archive"


## location latitude is: 32.955410
## location longitude is: -96.333717
## date: February 11, 2024 back to 2019.

class Weather():
    def __init__(self):
        self.location_latitude = 32.955410
        self.location_longitude = -96.333717
        self.month = 0
        self.monthDay = 0
        self.year = 0
        self.avgTemp = 0.0
        self.minTemp = 0.0
        self.maxTemp = 0.0
        self.avgWind = 0.0
        self.minWind = 0.0
        self.maxWind = 0.0
        self.sumPrecip = 0.0
        self.minPrecip = 0.0
        self.maxPrecip = 0.0


    def get_mean_temp(self):
        params = {
            "latitude": 32.95541,
            "longitude": -96.333717,
            "start_date": "2019-02-11",
            "end_date": "2024-02-11",
            "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"],
            "timezone": "America/Chicago"
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        daily = response.Daily()
        daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
        daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
        daily_temperature_2m_mean = daily.Variables(2).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}

        daily_data["temperature_2m_max"] = daily_temperature_2m_max
        daily_data["temperature_2m_min"] = daily_temperature_2m_min
        daily_data["temperature_2m_mean"] = daily_temperature_2m_mean

        temperature_dataframe = pd.DataFrame(data=daily_data)
        return temperature_dataframe


    def get_max_wind(self):
        params = {
            "latitude": 32.95541,
            "longitude": -96.333717,
            "start_date": "2019-02-11",
            "end_date": "2024-02-11",
            "daily": ["wind_speed_10m_max", "wind_speed_10m_min", "wind_speed_10m_mean"],
            "timezone": "America/Chicago"
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        daily = response.Daily()
        daily_wind_speed_10m_max = daily.Variables(0).ValuesAsNumpy()
        daily_wind_speed_10m_min = daily.Variables(1).ValuesAsNumpy()
        daily_wind_speed_10m_mean = daily.Variables(2).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}

        daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
        daily_data["wind_speed_10m_min"] = daily_wind_speed_10m_min
        daily_data["wind_speed_10m_mean"] = daily_wind_speed_10m_mean

        wind_dataframe = pd.DataFrame(data=daily_data)
        return wind_dataframe


    def get_precipSum(self):
        params = {
            "latitude": 32.95541,
            "longitude": -96.333717,
            "start_date": "2019-02-11",
            "end_date": "2024-02-11",
            "daily": "precipitation_sum",
            "timezone": "America/Chicago"
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        daily = response.Daily()
        daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}

        daily_data["precipitation_sum"] = daily_precipitation_sum

        precipitation_dataframe = pd.DataFrame(data=daily_data)

        return precipitation_dataframe


    def Fillin_data(self):
        Precip_data = self.get_precipSum()
        Temp_data = self.get_mean_temp()
        Wind_data = self.get_max_wind()

        MaxP = Precip_data['precipitation_sum'].max()
        MinP = Precip_data['precipitation_sum'].min()
        SumP = Precip_data['precipitation_sum'].sum()

        MaxT = Temp_data["temperature_2m_max"].max()
        MinT = Temp_data["temperature_2m_min"].min()
        AvgT = Temp_data["temperature_2m_mean"]

        MaxW = Wind_data["wind_speed_10m_max"].max()
        MinW = Wind_data["wind_speed_10m_min"].min()
        AvgW = Wind_data["wind_speed_10m_max"].mean()

        self.avgTemp = AvgT
        self.minTemp = MinT
        self.maxTemp = MaxT
        self.avgWind = AvgW
        self.minWind = MinW
        self.maxWind = MaxW
        self.sumPrecip = SumP
        self.minPrecip = MinP
        self.maxPrecip = MaxP


    def merge_data(self, temp_data, wind_data, precip_data):
        temp_db = temp_data
        wind_db = wind_data
        precip_db = precip_data
        dfs = [temp_db, wind_db, precip_db]
        df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='date'), dfs)
        return df_final


    def add_columns(self, data):
        self.Fillin_data()
        df = data
        df.reset_index(inplace=True)

        #Edit date to date format necessary
        df['day'] = df['date'].dt.day
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year

        df.drop(labels=['date'], axis=1, inplace=True)

        day = df['day']
        df.drop(labels=['day'], axis=1, inplace=True)
        df.insert(0, 'day', day)

        month = df['month']
        df.drop(labels=['month'], axis=1, inplace=True)
        df.insert(0, 'month', month)

        year = df['year']
        df.drop(labels=['year'], axis=1, inplace=True)
        df.insert(0, 'year', year)

        index = df['index']
        df.drop(labels=['index'], axis=1, inplace=True)
        df.insert(0, 'index', index)

        df.insert(loc=1, column='loca_latitude', value=self.location_latitude)
        df.insert(loc=2, column='loca_longitude', value=self.location_longitude)
        df.insert(loc=12, column='precipitation_min', value=self.minPrecip)
        df.insert(loc=13, column='precipitation_max', value=self.maxPrecip)

        return df
