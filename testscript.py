#!/usr/bin/env python3


import csv
import re

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()


CSV_FILE = 'Apulian_Database_dates.csv'
DB_NAME = 'apulian.sqlite'


class Vase(Base):
    __tablename__ = 'vases'

    id = Column(Integer, primary_key=True)
    fabric = Column(String(20))
    form = Column(String(20))
    subform = Column(String(20))
    produced_start = Column(String)
    produced_end = Column(String)

    # A.1. This is a many-to-one relationship, each vase has one painter, each
    # painter possibly many vases.
    painter_id = Column(Integer, ForeignKey('painters.id'))
    painter = relationship('Painter', back_populates='vases')

    location_id = Column(Integer, ForeignKey('locations.id'))
    location = relationship('Location', back_populates='vases')

    provenience = Column(String)
    trendall_ch = Column(Integer)
    trendall_no = Column(String)

    # B.1. Another many-to-one relationship, this time going the other
    # direction. Each vase has multiple sides, each side has only one vase.
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

    # A.2. This is the flip-side of the painter_id and painter properties
    # in Vase. There's no actual column for this in the database,
    # but this allows us to access the vases through this property
    # in the object.
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

    vases = relationship('Vase', back_populates='location')


class Image(Base):
    __tablename__ = 'images'

    id = Column(Integer, primary_key=True)
    name = Column(String(40))

    vase_id = Column(Integer, ForeignKey('vases.id'))
    vase = relationship('Vase', back_populates='images')


# C.1. Many-to-many relationship works through this table. Each theme can
# apply to multiple sides, and each side can have multiple themes represented.
# In the database, the relationship only exists in this table, not in the
# tables it references.
side_theme = Table('side_theme', Base.metadata,
                   Column('side_id', ForeignKey('sides.id'), primary_key=True),
                   Column('theme_id', ForeignKey('themes.id'),
                          primary_key=True))

side_musical_theme = Table('side_musical_theme', Base.metadata,
                           Column('side_id', ForeignKey('sides.id'),
                                  primary_key=True),
                           Column('theme_id', ForeignKey('musical_themes.id'),
                                  primary_key=True))


class Side(Base):
    __tablename__ = 'sides'

    id = Column(Integer, primary_key=True)
    identifier = Column(String(10))
    composition = Column(String(10))
    details = Column(String(1024))
    catalogue = Column(String(1024))

    # B.2. This is the flip side of Vase.sides.
    vase_id = Column(Integer, ForeignKey('vases.id'))
    vase = relationship('Vase', back_populates='sides')

    # C.2. This is one end of the many-to-many relationship.
    themes = relationship(
        'Theme',
        secondary=side_theme,
        back_populates='sides',
    )
    figures = relationship('Figure', back_populates='side')

    musical_themes = relationship(
        'MusicalTheme',
        secondary=side_musical_theme,
        back_populates='sides',
    )
    instruments = relationship(
        'InstrumentInstance',
        back_populates='side',
    )


class Theme(Base):
    __tablename__ = 'themes'

    id = Column(Integer, primary_key=True)
    name = Column(String(20))

    # C.3. This is the other end of the many-to-many relationship.
    sides = relationship(
        'Side',
        secondary=side_theme,
        back_populates='themes',
    )

    def __repr__(self):
        return '<Theme id={} name={}>'.format(
            self.id, self.name,
        )


musical_theme_instrument = Table(
    'musical_theme_instrument', Base.metadata,
    Column('musical_theme_id', ForeignKey('musical_themes.id'),
           primary_key=True),
    Column('instrument_id', ForeignKey('instruments.id'), primary_key=True),
)


class MusicalTheme(Base):
    __tablename__ = 'musical_themes'

    id = Column(Integer, primary_key=True)
    name = Column(String(20))

    sides = relationship(
        'Side',
        secondary=side_musical_theme,
        back_populates='musical_themes',
    )
    instrument_types = relationship(
        'Instrument',
        secondary=musical_theme_instrument,
        back_populates='themes',
    )
    instrument_instances = relationship(
        'InstrumentInstance',
        back_populates='theme'
    )


class Instrument(Base):
    __tablename__ = 'instruments'

    id = Column(Integer, primary_key=True)
    name = Column(String(20))

    themes = relationship(
        'MusicalTheme',
        secondary=musical_theme_instrument,
        back_populates='instrument_types',
    )
    instances = relationship('InstrumentInstance', back_populates='instrument')


class InstrumentInstance(Base):
    __tablename__ = 'instrument_instances'

    id = Column(Integer, primary_key=True)

    performer = Column(String(15))
    location = Column(String(10))
    action = Column(String(20))
    instr_count = Column(Integer)

    side_id = Column(Integer, ForeignKey('sides.id'))
    side = relationship('Side', back_populates='instruments')

    instrument_id = Column(Integer, ForeignKey('instruments.id'))
    instrument = relationship('Instrument', back_populates='instances')

    theme_id = Column(Integer, ForeignKey('musical_themes.id'))
    theme = relationship('MusicalTheme', back_populates='instrument_instances')


class Figure(Base):
    __tablename__ = 'figures'

    id = Column(Integer, primary_key=True)
    figure_type = Column(String(5))
    figure_count = Column(Integer, default=1)

    side_id = Column(Integer, ForeignKey('sides.id'))
    side = relationship('Side', back_populates='figures')


def read_csv(filename):
    """This reads the CSV files. """
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


# SCENE_TYPE_B,
# MUSICAL_SCENE_TYPE_B,TRENDALL_DESCRIPTION_B,SIDE_B_DETAILS,
# SIDE_B_INSTRUMENTS_AND_NUMBERS,SIDE_B_PERFORMERS,SIDE_B_PERFORMER_LOCATION,
# SIDE_B_PERFORMER_ACTION,SIDE_B_NUMBER_OF_FIGURES,SIDE_B_COMPOSITION,

# IMAGE_IDS,NOTES,PUBLICATION,CATEGORY_1,CATEGORY_2
class RowAdapter:

    def __init__(self):
        self.vases = []
        self.painters = {}
        self.locations = {}
        self.themes = {}
        self.musical_themes = {}

    def adapt_vase(self, row):
        """Adapts a CSV row into a sequence of database objects."""
        objects = []

        trendall_ch, trendall_no = self._parse_trendall(row['TRENDALL_ID'])

        vase = Vase(
            fabric=row['FABRIC'],
            form=row['FORM'],
            subform=row['SUB-FORM'],
            produced_start=row['START_DATE'],
            produced_end=row['END_DATE'],
            provenience=row['PROVENIENCE'],
            trendall_ch=trendall_ch,
            trendall_no=trendall_no,
        )
        objects.append(vase)
        self.vases.append(vase)

        vase.painter = self._get_cached(
            self.painters, objects, row['PAINTER'],
            lambda: Painter(name=row['PAINTER'])
        )
        vase.location = self._get_cached(
            self.locations, objects, row['LOCATION_CITY'],
            lambda: Location(
                city_name=row['LOCATION_CITY'],
                collection_name=row['COLLECTION_NAME'],
                collection_id=row['COLLECTION_ID'],
            ),
        )

        for image_id in self._parse_images(row['IMAGE_SERIES']):
            image = Image(name=str(image_id), vase=vase)
            objects.append(image)

        vase.sides.append(self._adapt_side('A', row, objects))
        vase.sides.append(self._adapt_side('B', row, objects))

        return objects

    def _parse_trendall(self, trendall_id):
        trendall_ch, trendall_no = trendall_id.split('.')
        trendall_ch = int(trendall_ch)
        trendall_no = trendall_no.strip()
        return (trendall_ch, trendall_no)

    def _parse_images(self, image_str):
        numbers = set()

        for number_range_str in image_str.split(','):
            number_range = number_range_str.split('-')
            if len(number_range) == 1:
                numbers.add(int(number_range))
            else:
                start_str, end_str = number_range
                start = int(start_str)
                end = int(end_str)
                if end < start:
                    end = int(start_str[:-len(end_str)] + end_str)
                numbers.add(start)
                numbers.add(end)

        return sorted(numbers)

# SIDE_A_INSTRUMENTS_AND_NUMBERS,SIDE_A_PERFORMERS,SIDE_A_PERFORMER_LOCATION,
# SIDE_A_PERFORMER_ACTION,SIDE_A_NUMBER_OF_FIGURES,SIDE_A_COMPOSITION,
    def _adapt_side(self, side_id, row, objects):
        side_data = self._get_side_data(row, side_id)

        side = Side(
            identifier=None,
            composition=side_data['composition'],
            details=side_data['details'],
            catalogue=side_data['catalogue_description'],
        )

        for theme_name in side_data['scene_type']:
            theme = self._get_cached(
                self.themes, objects, theme_name,
                lambda: Theme(name=theme_name),
            )
            side.themes.append(theme)
        for theme_name in side_data['musical_scene_type']:
            theme = self._get_cached(
                self.musical_themes, objects, theme_name,
                lambda: MusicalTheme(name=theme_name),
            )
            side.musical_themes.append(theme)

        # figures
        # instrument instance

        return side

    def _get_side_data(self, row, side_id):
        return {
            'scene_type': self._parse_scene_type(
                row['SCENE_TYPE_' + side_id]),

            # parse like scene_type, pull off last component and make an
            # Instrument, associated with both MusicalThemes that precede it.

            'musical_scene_type': self._parse_scene_type(
                row['MUSICAL_SCENE_TYPE_' + side_id]),
            'catalogue_description': row['CATALOGUE_DESCRIPTION_' + side_id],
            'details': row['SIDE_%s_DETAILS' % side_id],

            # throw away these numbers, get them by counting occurrences in
            # performers column

            'instruments_and_numbers': self._parse_instr_nos(
                row['SIDE_%s_INSTRUMENTS_AND_NUMBERS' % side_id]),
            # PERFORMER(INST)
            'performers': row['SIDE_%s_PERFORMERS' % side_id],
            # LOCATION(INST)
            'performer_location': row['SIDE_%s_PERFORMER_LOCATION' % side_id],
            # ACTION(INST)
            'performer_action': row['SIDE_%s_PERFORMER_ACTION' % side_id],
            # COUNT_FIGURE
            'figure_count': row['SIDE_%s_NUMBER_OF_FIGURES' % side_id],
            'composition': row['SIDE_%s_COMPOSITION' % side_id],
        }

    def _parse_scene_type(self, scene_type, with_trailing=False):
        """\
        Splits a scene type field by comma then by parenthesis, flattening
        it into one list.

        >>> adapter = RowAdapter()
        >>> adapter._parse_scene_type('PROCESSION')
        ['PROCESSION']
        >>> adapter._parse_scene_type('PROCESSION (DIONYSIAC), MYTHOLOGICAL')
        ['PROCESSION', 'DIONYSIAC', 'MYTHOLOGICAL']

        If with_trailing is True, then each item in the list is a tuple
        pair of the item and the trailing parenthesis for that comma-
        separated segment.

        >>> adapter._parse_scene_type('PROCESSION')
        [('PROCESSION', None)]
        >>> adapter._parse_scene_type('PROCESSION(AU)')
        [('PROCESSION', 'AU')]
        >>> adapter._parse_scene_type(
                'ATTENDANT (DIONYSIAC)(TYM), ATTENDANT (DIONYSIAC)(CYM)'
                )
        [('ATTENDANT', 'TYM'), ('DIONYSIAC', 'TYM'), ('ATTENDANT', 'CYM'), ('DIONYSIAC', 'CYM')]

        """
        names = []
        for s_type in scene_type.split(','):
            theme_names = [
                    tname.strip(' ()')
                    for tname in s_type.split('(')
                    ]
            if with_trailing:
                if len(theme_names) > 1:
                    trailing = theme_names.pop()
                else:
                    trailing = None
                theme_names = [(tname, trailing) for tname in theme_names]
        return names

    def _parser_perf_info(self, performer_info):
        pass

    def _parse_instr_nos(self, value):
        inos = []
        for ino in value.split(','):
            count, instr = ino.split('_')
            count = int(count)
            instr = instr.strip('()')
            inos.append((instr, count))
        return inos

    def _get_cached(self, cache, objects, key, ctor):
        obj = cache.get(key)
        if obj is None:
            obj = ctor()
            objects.append(obj)
            cache[key] = obj
        return obj

    def get_shared_objects(self):
        return self.painters.items()


def main():
    engine = sqlalchemy.create_engine(
        'sqlite:///{}'.format(DB_NAME), echo=True
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    adapter = RowAdapter()
    for row in read_csv(CSV_FILE):
        for obj in adapter.adapt_vase(row):
            session.add(obj)

    session.commit()


if __name__ == '__main__':
    main()
