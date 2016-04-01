#!/usr/bin/env python3


"""The entry point to populating the database from the CSV file."""


import os
import sys

from apulian.adapter import RowAdapter
from apulian.models import bootstrap
from apulian.utils import read_csv


CSV_FILE = 'Apulian_Database_dates.csv'
DB_NAME = 'apulian.sqlite'


# TODO: IMAGE_IDS,NOTES,PUBLICATION,CATEGORY_1,CATEGORY_2


def main():
    """The entry point to populating the database. """
    if '-X' in sys.argv or '--clear' in sys.argv:
        print('Removing {}'.format(DB_NAME))
        os.remove(DB_NAME)

    make_session = bootstrap(
        'sqlite:///{}'.format(DB_NAME), echo=True,
    )
    session = make_session()

    adapter = RowAdapter()
    for row in read_csv(CSV_FILE):
        for obj in adapter.adapt_vase(row):
            session.add(obj)

    session.commit()


if __name__ == '__main__':
    main()
