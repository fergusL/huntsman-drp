"""Script to run daily to produce DQ from new imaging.
"""
from dateutil.parser import parse as parse_date
import datetime
from huntsman.drp.metadb import MetaDatabase
import argparse

from huntsman.drp.dataquality import (generate_science_data_quality,
                                      generate_calib_data_quality)


def main(meta_db_location, date=None, date_range=1):
    """Main function for daily run script, which examines
    data taken in the last day and generates data quality
    information, which is put into the Meta DB.

    Args:
        meta_db_location (str): File location of Meta DB.
        date (str, optional): Date to search for files.
        date_range (int, optional): Number of days to search before date arg.
    """
    mdb = MetaDatabase(meta_db_location)

    if date is not None:
        date = datetime.today()
    date_parsed = parse_date(date)
    date_range = datetime.timedelta(days=date_range)

    generate_science_data_quality(mdb,
                                  date_min=date_parsed - date_range,
                                  date_max=date_parsed - date_range)

    generate_calib_data_quality(mdb,
                                date_min=date_parsed - date_range,
                                date_max=date_parsed - date_range)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('meta-db', type=str, help='Meta database file location.')
    args = parser.parse_args()
    meta_db_location = args.metadb

    main(meta_db_location)
