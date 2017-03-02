
"""\
Data models
"""

__all__ = [
    'bootstrap',
    'Vase',
    'Painter',
    'Location',
    'Image',
    'Side',
    'Theme',
    'Instrument',
    'InstrumentInstance',
    'Figure',
    ]


import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()


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

    def __repr__(self):
        return '<Location id={}, city={}, collection={}>'.format(
            self.id, self.city_name, self.collection_name,
            )


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
side_theme = Table(
    'side_theme', Base.metadata,
    Column('side_id', ForeignKey('sides.id'), primary_key=True),
    Column('theme_id', ForeignKey('themes.id'), primary_key=True),
    )


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

    # C.2. Each side links to an unspecified number of themes.
    themes = relationship(
        'Theme',
        secondary=side_theme,
        back_populates='sides',
    )
    figures = relationship('Figure', back_populates='side')
    instruments = relationship(
        'InstrumentInstance',
        back_populates='side',
    )


instance_theme = Table(
    'instance_theme', Base.metadata,
    Column('instrument_instance_id', ForeignKey('instrument_instances.id'), primary_key=True),
    Column('theme_id', ForeignKey('themes.id'), primary_key=True),
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

    instruments = relationship(
        'InstrumentInstance',
        secondary=instance_theme,
        back_populates='themes'
    )

    def __repr__(self):
        return '<Theme id={} name={}>'.format(
            self.id, self.name,
        )


class Instrument(Base):
    __tablename__ = 'instruments'

    id = Column(Integer, primary_key=True)
    name = Column(String(20))

    instances = relationship(
        'InstrumentInstance',
        back_populates='instrument',
        )


class InstrumentInstance(Base):
    __tablename__ = 'instrument_instances'

    id = Column(Integer, primary_key=True)

    performer = Column(String(15))
    location = Column(String(10))
    action = Column(String(20))

    side_id = Column(Integer, ForeignKey('sides.id'))
    side = relationship('Side', back_populates='instruments')

    themes = relationship(
        'Theme',
        secondary=instance_theme,
        back_populates='instruments',
    )

    instrument_id = Column(Integer, ForeignKey('instruments.id'))
    instrument = relationship('Instrument', back_populates='instances')


class Figure(Base):
    __tablename__ = 'figures'

    id = Column(Integer, primary_key=True)
    figure_type = Column(String(5))
    figure_count = Column(Integer, default=1)

    side_id = Column(Integer, ForeignKey('sides.id'))
    side = relationship('Side', back_populates='figures')


def bootstrap(uri, **kwargs):
    """\
    This bootstraps the ORM system and returns the `Session` class constructor.
    """
    engine = sqlalchemy.create_engine(uri, **kwargs)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
