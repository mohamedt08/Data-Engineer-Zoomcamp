import click
import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.request import urlretrieve


@click.command()
@click.option("--year", type=int, default=2025, show_default=True, help="Year of the trip data")
@click.option("--month", type=int, default=11, show_default=True, help="Month of the trip data (1-12)")
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-password", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default="5432", show_default=True, help="Postgres port")
@click.option("--pg-database", default="ny_taxi", show_default=True, help="Postgres database")
@click.option("--trip-table-name", default="green_taxi_data", show_default=True, help="Trips table name")
@click.option("--zone-table-name", default="taxi_zones_data", show_default=True, help="Zones table name")
def run(year, month, pg_user, pg_password, pg_host, pg_port, pg_database, trip_table_name, zone_table_name):
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
    url = f"{prefix}taxi_zone_lookup.csv"
    taxi_zones_df = pd.read_csv(url)

    first = True

    trip_file = f"green_tripdata_{year}-{month:02d}.parquet"

    # download parquet if not present locally
    if not os.path.exists(trip_file):
        trip_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{trip_file}"
        print(f"{trip_file} not found locally — downloading from: {trip_url}")
        urlretrieve(trip_url, trip_file)

    trip_df = pd.read_parquet(trip_file)

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
    )

    if first:
        trip_df.head(0).to_sql(name=trip_table_name, con=engine, if_exists="replace", index=False)
        taxi_zones_df.head(0).to_sql(name=zone_table_name, con=engine, if_exists="replace", index=False)
        first = False

    trip_df.to_sql(name=trip_table_name, con=engine, if_exists="append", index=False)
    taxi_zones_df.to_sql(name=zone_table_name, con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    run()
