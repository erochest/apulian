#! /usr/bin/env python3


"""This queries the file."""


from apulian.models import *
from populate import DB_NAME


def main():
    make_session = bootstrap(
        'sqlite:///{}'.format(DB_NAME),
        )
    session = make_session()

    for vase in session.query(Vase).order_by(Vase.produced_start):
        print(vase)
        print(vase.painter)
        print(vase.location)
        for side in vase.sides:
            print(side.identifier)
            for i in side.instruments:
                print(i.theme.name, i.instrument.name, i.performer,
                      i.location, i.action)
        print()


if __name__ == '__main__':
    main()
