#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db):

    # Read a sample of the data
    url_green = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    df_green = pd.read_parquet(url_green)

    # Read a sample of the data
    url_zones = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    df_zones = pd.read_csv(url_zones)

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(pd.io.sql.get_schema(df_green, name='green_taxi_data', con=engine))
    print(pd.io.sql.get_schema(df_zones, name='taxi_data_zones', con=engine))


    df_green.head(0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')
    df_zones.head(0).to_sql(name='taxi_data_zones', con=engine, if_exists='replace')

    df_green.to_sql(
            name="green_taxi_data",
            con=engine,
            if_exists="append"
        )


    df_zones.to_sql(
            name="taxi_data_zones",
            con=engine,
            if_exists="append"
        )

if __name__ == '__main__':
    run()
