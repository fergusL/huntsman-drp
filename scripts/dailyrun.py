from dateutil.parser import parse as parse_date
import datetime
from huntsman.drp.meta import MetaDatabase
import argparse

from huntsman.drp.dataquality import (generate_science_data_quality,
                                      generate_calib_data_quality)


def main(meta_db_location, date=None, date_range=1):

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
