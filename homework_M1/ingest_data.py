
import pandas as pd
from sqlalchemy import create_engine



def run():
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/'
    url= f'{prefix}taxi_zone_lookup.csv'
    taxi_zones_df = pd.read_csv(url)

    year = 2025
    month = 11
    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_port = '5432'
    pg_database = 'ny_taxi'

    first= True

    trip_table_name = 'green_taxi_data'
    zone_table_name = 'taxi_zones_data'

    trip_df = pd.read_parquet(f'green_tripdata_{year}-{month}.parquet')

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')

    if first:

        trip_df.head(0).to_sql(name=trip_table_name, con=engine, if_exists='replace')
        taxi_zones_df.head(0).to_sql(name=zone_table_name, con=engine, if_exists='replace')
        first= False

    trip_df.to_sql(name=trip_table_name, con=engine, if_exists='append')
    taxi_zones_df.to_sql(name=zone_table_name, con=engine, if_exists='append')


if __name__ == "__main__":
    run()
