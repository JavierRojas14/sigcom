# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta
from pathlib import Path

import click
import pandas as pd
from dotenv import find_dotenv, load_dotenv


def get_year_and_month():
    while True:
        try:
            year = int(input("Enter a year (e.g., 2023): "))
            month = int(input("Enter a month (1-12): "))

            if 1 <= month <= 12:
                print(f"> You selected year: {year}, month: {month}")
                return year, month
            else:
                print("Invalid month. Please enter a number between 1 and 12.")
        except ValueError:
            print("Invalid input. Please enter a valid number for year and month.")


def get_first_and_last_day(year, month):
    first_day = datetime(year, month, 1)
    next_month = first_day.replace(month=first_day.month + 1, day=1)
    last_day = next_month - timedelta(days=1)
    last_day = last_day.replace(hour=23, minute=59, second=59)

    print(f"> First day of {year}-{month:02}: {first_day}")
    print(f"> Last day of {year}-{month:02}: {last_day}")

    return first_day, last_day


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")
    anio, mes = get_year_and_month()
    primer_dia_periodo, ultimo_dia_periodo = get_first_and_last_day(anio, mes)


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
