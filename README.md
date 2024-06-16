# API_SQL_Pipeline
Creating a pipeline using an API to upload into a SQL table.

************Requirements for Weather API************

Requirements for SourceCode include:
flask_restful: resource
openmeteo_requests
requests_cache
pandas
retry_requests: retry
functools

Requirements for app include:
SourceCode
sqlite3

Requirements for test include:
pytest


************inputs and outputs needed************
To create the database, the method db_create will need to be run, or the class weatherDatabase will need to be called
using sqlalchemy.

Everything can run on app.py, there is no need to move between the SourceCode and app.py.

The only inputs  are to call the methods. If a query needs to be done, you can either query the dataframe
object or query the sql database.
The descriptions below will go into further detail on the inputs needed for specific commands and the outputs that are
displayed.


************Description of Code by group************

***SourceCode.py***

From lines 10 - 12, we are establishing a cache session in order to catch any errors that occur when pulling data
through the API code. On line 15, we are establishing the url variable, which will be used in a lot of code. Thus, it is
declared outside the classes and methods.

From line 18 onwards, we have the class declarations and methods. the Weather class is the overall class holding the variables,
along with the functions that will call the weather API data. It is all on a daily basis.

get_mean_temp will request three variables, including the mean. It requests the max, the min, and the mean. Then it formats
it into a small dataframe object and returns the dataframe.

get_max_wind will request three variables, including the max. It requests the max, the min, and the mean. Then it formats
the data into a dataframe object and returns that dataframe object.

get_precipSum will request only the precipitation sum from the weather API. This is because the weather API did not have
other variables for the precipitation max and min. It returns a dataframe object containing just the precipitation sum.

Fillin_data begins to format the data into the class variables that were declared in __init__ at the beginning of the weather
class declaration. It finds the max, min, and sum for the precipitation. Then the max, min, and mean for the temperature data.
Then the max, min, and mean for the wind data. Then it initializes the information into the beginning variables.

merge_data combines the dataframes gathered into one dataframe object. It returns the dataframe object.

add_columns re-formats the dataframe object and adds the missing data: the chosen latitude, longitude, precipitation max, and
precipitation min. It also re-formats the date format to be split into day, month, and year. Finally, it returns the dataframe
as a finalized product.


***app.py***

For line 10, we first declare the base object that will be used to create our database table.

In line 12 - 24, we are creating an engine object then moving that into a session. A message will be displayed "Connection
created successfully." if the connection was complete. Otherwise, the except clause will catch the error and display
"Connection could not be made due to the following error:", then it will display the error.

In lines 27 - 35, we are calling the class and the class methods to get the data that we need. This produces the final
variable, final_df, that is our main dataframe object.

From here onwards, we have functions that are declared to create the database and query the database.

db_create will output the full database. The first line of code on line 39 will create any databases defined with the base
argument in the class parameters. The SQL table created will be called "Weather_Database" but the file will be called
weatherData.db due to the previous connection established. This also fills in all of the data needed, thus we can
immediately access the database and it's contents using our sql queries through python, or through a SQL database browser.

db_all_data() will output all of the data from the sql database in full.

db_sql_query_example will execute a simple query against the "day" column, based on the parameter given in the argument.
*Note, for future queries the SQL code can be edited for different parameters.

dataframe_query is a query system to query the dataframe object itself. This is for verification purposes, so that we can
confirm that the sql query correctly pulled the information we need. It will have menu prompts which ask for an input. You
can input: show list, show database, stop, and the query itself. The output will either be a list of columns, the full database,
the message "Exited query successfully", or the query results respectfully.

weatherDatabase(base) is a class that uses the base argument from the sqlalchemy ORM module. This is the actual database
with all columns necessary.

From line 104 onwards, we have the code to test and display the results for video demonstration.


***test.py***

test.py has four functions within the class TestWeather that test the code within SourceCode.py.

If you run TestWeather, it will run all subsequent tests for the code.

test_DF_object will test to assert that when we call SourceCode to create the dataframe, it creates a dataframe object.

test_get_mean_temp will test to make sure that the first row of the temperature dataframe is a string type.

test_get_max_wind will test to make sure that the first row of the wind dataframe is a string type.

test_get_PrecipSum will test to make sure that the first row of the precipitation dataframe is a string type.


************Sources************


Zippenfenig, P. (2023). Open-Meteo.com Weather API [Computer software]. Zenodo.
https://doi.org/10.5281/ZENODO.7970649

Hersbach, H., Bell, B., Berrisford, P., Biavati, G., Horányi, A., Muñoz Sabater, J., Nicolas, J.,
Peubey, C., Radu, R., Rozum, I., Schepers, D., Simmons, A., Soci, C., Dee, D., Thépaut, J-N. (2023).
ERA5 hourly data on single levels from 1940 to present [Data set]. ECMWF.
https://doi.org/10.24381/cds.adbb2d47

Muñoz Sabater, J. (2019). ERA5-Land hourly data from 2001 to present [Data set]. ECMWF.
https://doi.org/10.24381/CDS.E2161BAC

Schimanke S., Ridal M., Le Moigne P., Berggren L., Undén P., Randriamampianina R., Andrea U., Bazile E., Bertelsen A.,
Brousseau P., Dahlgren P., Edvinsson L., El Said A., Glinton M., Hopsch S., Isaksson L., Mladek R., Olsson E., Verrelle A.,
Wang Z.Q. (2021).
CERRA sub-daily regional reanalysis data for Europe on single levels from 1984 to present [Data set]. ECMWF.
https://doi.org/10.24381/CDS.622A565A
