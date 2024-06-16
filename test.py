import pandas as pd
import pytest
import SourceCode


Weather_db = SourceCode.Weather()

temp_data = Weather_db.get_mean_temp()
wind_data = Weather_db.get_max_wind()
precip_data = Weather_db.get_precipSum()

data = Weather_db.merge_data(temp_data, wind_data, precip_data)
final_df = Weather_db.add_columns(data)

class TestWeather:

    def test_DF_object(self):
        assert(isinstance(final_df, pd.DataFrame))


    def test_get_mean_temp(self):
        ## Tests to see if the columns are string type
        Test_data = SourceCode.Weather.get_mean_temp(self=None)
        for row in Test_data:
            assert(isinstance(row[0], str))


    def test_get_max_wind(self):
        Test_data = SourceCode.Weather.get_max_wind(self=None)
        for row in Test_data:
            assert(isinstance(row[0], str))


    def test_get_PrecipSum(self):
        Test_data = SourceCode.Weather.get_precipSum(self=None)
        for row in Test_data:
            assert(isinstance(row[0], str))
