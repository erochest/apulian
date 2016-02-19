#!/usr/bin/env python3


import datetime
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


DB_NAME = 'apulian.sqlite'


class Vases(Base):
    __tablename__ ='vases'

    id = Column(Integer, primary_key=True)
    fabric = Column(String(20))
    form = Column(String(20))
    subform = Column(String(20))
    produced_start = Column(String)
    produced_end = Column(String)
    # TODO: painter
    # TODO: location
    provenance = Column(String)
    trendall_ch = Column(Integer)
    trendall_no = Column(String)
    # TODO: side
    # TODO: image

    def __repr__(self):
        return "<Vases id={} fabric={} form={} subform={}>".format(
            self.id, self.fabric, self.form, self.subform,
        )


def main():
    engine = sqlalchemy.create_engine(
        'sqlite:///{}'.format(DB_NAME), echo=True
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # vase = Vases(
    #     fabric='Apulian',
    #     form='Krater',
    #     subform='Volute',
    #     produced_start='-0400',
    #     produced_end='-0350',
    # )
    # session.add(vase)
    for vase in session.query(Vases).order_by(Vases.fabric):
        print(vase)


if __name__ == '__main__':
    main()
