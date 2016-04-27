#!/usr/bin/env python3


"""\
This queries JStore to get references for all the items in the apulian
database.
"""


import argparse
import sys

from apulian.models import bootstrap, Vase
from populate import DB_NAME


N = 10


def jstor(query_str, n):
    """Takes a query string and returns up to n results from dfr.jstor.org. """


def make_searches(vase):
    """Takes a vase and returns queries for it."""
    params = {
        'ch': vase.trendall_ch,
        'no': vase.trendall_no,
        'city': vase.location.city_name,
        'col': vase.location.collection_name,
        'id':  vase.location.collection_id,
        }
    return [
        'trendall {ch}.{no}'.format(**params),
        'trendall {ch}/{no}'.format(**params),
        '{city} {id}'.format(**params),
        '{col} {id}'.format(**params),
        ]


def parse_args(argv=None):
    """Parse command-line arguments."""
    argv = argv if argv is not None else sys.argv[1:]

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('-n', '--n', dest='n', action='store', default=N, type=int,
                   help='The number of items to return in each search. '
                        'Default = {}.'.format(N))

    args = p.parse_args()

    return args


def main():
    """The main entrypoint for this process."""
    args = parse_args()

    make_session = bootstrap(
        'sqlite:///{}'.format(DB_NAME),
        )
    session = make_session()

    for vase in session.query(Vase).order_by(Vase.produced_start):
        for query_str in make_searches(vase):
            pass


if __name__ == '__main__':
    main()
