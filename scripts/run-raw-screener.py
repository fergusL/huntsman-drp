""" Run the initial data quality screener. The task of the initial data quality screener is to
extract information like whether a file has wcs/if the file is corrupt and other basic
metadata. This metadata is then stored in the database quality table.
"""
from huntsman.drp.quality.screening import Screener

if __name__ == "__main__":

    screener = Screener()
    screener.start()
