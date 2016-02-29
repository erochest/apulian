#!/usr/bin/env python3


import random
from faker import Faker
import datetime
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()


DB_NAME = 'apulian.sqlite'


class Vase(Base):
    __tablename__ = 'vases'

    id = Column(Integer, primary_key=True)
    fabric = Column(String(20))
    form = Column(String(20))
    subform = Column(String(20))
    produced_start = Column(String)
    produced_end = Column(String)

    painter_id = Column(Integer, ForeignKey('painters.id'))
    painter = relationship('Painter', back_populates='vases')

    location_id = Column(Integer, ForeignKey('locations.id'))
    location = relationship('Location', back_populates='vases')

    provenance = Column(String)
    trendall_ch = Column(Integer)
    trendall_no = Column(String)

    sides = relationship('Side', back_populates='vase')
    images = relationship('Image', back_populates='vase')

    def __repr__(self):
        return "<Vase id={} fabric={} form={} subform={}>".format(
            self.id, self.fabric, self.form, self.subform,
        )


class Painter(Base):
    __tablename__ = 'painters'

    id = Column(Integer, primary_key=True)
    name = Column(String(40))

    vases = relationship('Vase', back_populates='painter')

    def __repr__(self):
        return '<Painter id={}, name={}, vase_count={}>'.format(
            self.id, self.name, len(self.vases),
        )


class Location(Base):
    __tablename__ = 'locations'

    id = Column(Integer, primary_key=True)
    city_name = Column(String(40))
    collection_name = Column(String(40), nullable=True)
    collection_id = Column(String(20), nullable=True)
    # for St. Petersburg or Leningrad, depending on your politics
    collection_id_secondary = Column(String(20), nullable=True)

    vases = relationship('Vase', back_populates='location')


class Image(Base):
    __tablename__ = 'images'

    id = Column(Integer, primary_key=True)
    name = Column(String(40))

    vase_id = Column(Integer, ForeignKey('vases.id'))
    vase = relationship('Vase', back_populates='images')


side_theme = Table('side_theme', Base.metadata,
    Column('side_id', ForeignKey('sides.id'), primary_key=True),
    Column('theme_id', ForeignKey('themes.id'), primary_key=True),
)


class Side(Base):
    __tablename__ = 'sides'

    id = Column(Integer, primary_key=True)
    identifier = Column(String(10))
    composition = Column(String(10))

    vase_id = Column(Integer, ForeignKey('vases.id'))
    vase = relationship('Vase', back_populates='sides')

    themes = relationship(
        'Theme',
        secondary=side_theme,
        back_populates='sides',
    )
    figures = relationship('Figure', back_populates='side')


class Theme(Base):
    __tablename__ = 'themes'

    id = Column(Integer, primary_key=True)
    name = Column(String(20))

    sides = relationship(
        'Side',
        secondary=side_theme,
        back_populates='themes',
    )

    def __repr__(self):
        return '<Theme id={} name={}>'.format(
            self.id, self.name,
        )


class Figure(Base):
    __tablename__ = 'figures'

    id = Column(Integer, primary_key=True)
    figure_type = Column(String(5))
    figure_count = Column(Integer, default=1)

    side_id = Column(Integer, ForeignKey('sides.id'))
    side = relationship('Side', back_populates='figures')


def main():
    engine = sqlalchemy.create_engine(
        'sqlite:///{}'.format(DB_NAME), echo=True
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    fake = Faker('el_GR')
    painters = []
    for n in range(random.randint(0, 10)):
        p = Painter(name=fake.name())
        painters.append(p)
        session.add(p)

    vases = []
    for n in range(random.randint(0, 100)):
        start = random.randint(-400, -300)
        v = Vase(
            fabric=fake.name(),
            form=fake.name(),
            subform=fake.name(),
            produced_start=str(start),
            produced_end=str(random.randint(start, -300)),
        )
        v.painter = random.choice(painters)
        vases.append(v)
        session.add(v)

    session.commit()

    for p in session.query(Painter).all():
        print(p)

    side = Side(identifier='a', composition='poor')
    music = Theme(name='music')
    pirates = Theme(name='pirates')

    session.add(side)
    session.add(music)
    session.add(pirates)

    side.themes += [music, pirates]

    session.commit()

    for theme in side.themes:
        print(theme)


if __name__ == '__main__':
    main()
