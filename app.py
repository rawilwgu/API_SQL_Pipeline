from sqlalchemy import Column, Integer, String, Float, create_engine, event
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import sqlite3
import psycopg2
import os
import SourceCode
from IPython.display import display

base = sqlalchemy.orm.declarative_base()

# Establishes connection and catches any exception errors
def get_connection():
    return create_engine(url='sqlite:///weatherData.db')


if __name__ == '__main__':
    try:
        engine = get_connection()
        Session = sessionmaker(bind=engine)
        session = Session()
        print(f"Connection created successfully.")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)


## Calling instances
Weather_db = SourceCode.Weather()

temp_data = Weather_db.get_mean_temp()
wind_data = Weather_db.get_max_wind()
precip_data = Weather_db.get_precipSum()

data = Weather_db.merge_data(temp_data, wind_data, precip_data)
final_df = Weather_db.add_columns(data)


def db_create():
    base.metadata.create_all(engine)
    final_df.to_sql('Weather_Database', engine, if_exists='append')
    session.commit()
    print('Database file created and filled in successfully')


def db_all_data():
    con = sqlite3.connect('weatherData.db')
    cur = con.cursor()
    cur.execute("SELECT * FROM Weather_Database")
    rows = cur.fetchall()
    for row in rows:
        print(row)


def db_sql_query_example(param):
    con = sqlite3.connect('weatherData.db')
    cur = con.cursor()
    data = cur.execute("SELECT * FROM Weather_Database WHERE day=?",
                (param,))
    return data


def dataframe_query():
    df = final_df
    column_list = ['index', 'loca_latitude', 'loca_longitude', 'year', 'month', 'day', 'temperature_2m_max',
           'temperature_2m_min', 'temperature_2m_mean', 'wind_speed_10m_max', 'wind_speed_10m_min',
           'wind_speed_10m_mean', 'precipitation_min', 'precipitation_max', 'precipitation_sum']
    print('Weather Database Query')
    print('Type your query without any quotes. Or type one of the following commands')
    print('Type \'show list\' to show all columns')
    print('Type \'show database\' to show database')
    print('Type \'stop\' to exit')
    print('Enter query here:')
    querystring = str(input())
    if querystring == 'show list':
        for x in column_list:
            print(x, end='\n')
    elif querystring == 'stop':
        print('Exited query successfully!')
    elif querystring == 'show database':
        print(df)
    else:
        print(df.query(querystring))


## Database creation
class weatherDatabase(base):
    __tablename__ = 'Weather_Database'
    level_0 = Column(Integer)
    index = Column(Integer, primary_key=True)
    loca_latitude = Column(Float)
    loca_longitude = Column(Float)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    temperature_2m_max = Column(Float)
    temperature_2m_min = Column(Float)
    temperature_2m_mean = Column(Float)
    wind_speed_10m_max = Column(Float)
    wind_speed_10m_min = Column (Float)
    wind_speed_10m_mean = Column(Float)
    precipitation_min = Column(Float)
    precipitation_max = Column(Float)
    precipitation_sum = Column(Float)

# first
display(final_df)

# second
db_create()

# third
db_all_data()

# fourth
for row in db_sql_query_example(13):
    print(row)

# fifth
dataframe_query()
