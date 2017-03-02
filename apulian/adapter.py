
"""\
This adapter the input of the CSV file into the data model.
"""


from collections import defaultdict

from apulian.models import Vase, Painter, Location, Image, Side, Theme, \
        Instrument, InstrumentInstance, Figure
from apulian.utils import take_digits


class RowAdapter:

    def __init__(self):
        self.vases = []
        self.painters = {}
        self.locations = {}
        self.themes = {}
        self.instruments = {}

    def adapt_vase(self, row):
        """Adapts a CSV row into a sequence of database objects."""
        objects = []

        try:
            trendall_ch, trendall_no = self._parse_trendall(row['TRENDALL_ID'])

        except:
            print('INVALID TRENDALL ID: "{}"'.format(row['TRENDALL_ID']))

        else:
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

            vase.sides.append(
                self._adapt_side('A', row, objects, row['TRENDALL_ID']),
                )
            vase.sides.append(
                self._adapt_side('B', row, objects, row['TRENDALL_ID']),
                )

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
            # This is because there is at least one like "123-"
            if len(number_range) == 1 or not number_range[1]:
                if number_range[0].isdigit():
                    numbers.add(int(number_range[0]))
            else:
                try:
                    start_str, end_str = number_range
                    start = int(start_str)
                    # This split and index protects from things like
                    # "1 (actual)"
                    end = int(end_str.split()[0])

                    if end < start:
                        end_str = start_str[:-len(end_str)] + end_str
                        end = int(end_str.split()[0])
                    numbers.add(start)
                    numbers.add(end)
                except:
                    print('ERROR on "{}"'.format(image_str))
                    raise

        return sorted(numbers)

    def _adapt_side(self, side_id, row, objects, trendall_id):
        # TODO: how are sides verified? under what conditions is a side not
        # created? if there are no instrument instances?

        side_data = self._get_side_data(row, side_id)

        side = Side(
            identifier=side_id,
            composition=side_data['composition'],
            details=side_data['details'],
            catalogue=side_data['catalogue_description'],
        )
        objects.append(side)

        # First, let's make sure the themes and instruments we want to connect
        # already exist.
        side_themes = {}
        for (theme_name, _) in side_data['scene_type']:
            theme = self._get_cached(
                self.themes, objects, theme_name,
                lambda: Theme(name=theme_name),
            )
            side_themes[theme_name] = theme
        side.themes.extend(side_themes.values())
        for (_, inst_name) in side_data['musical_scene_type']:
            self._get_cached(
                self.instruments, objects, inst_name,
                lambda: Instrument(name=inst_name),
            )

        inst_info = list(zip(
            side_data['performers'],
            side_data['performer_location'],
            side_data['performer_action'],
            ))

        mss = defaultdict(list)
        for ms in side_data['musical_scene_type']:
            if ms[0] in self.themes:
                mss[ms[1]].append(self.themes[ms[0]])

        for (p, pl, pa) in inst_info:
            if p[1] in mss and p[1] == pl[1] == pa[1]:
                # TODO: performer, location, and action can be NULL. '?' should
                # be read as NULL. See 1.99 A.
                inst_inst = InstrumentInstance(
                    performer=p[0],
                    location=pl[0],
                    action=pa[0],
                    side=side,
                    themes=mss[p[1]],
                    instrument=self.instruments[ms[1]],
                    )
                objects.append(inst_inst)
            elif p[1] and not (p[1] in mss and p[1] == pl[1] == pa[1]):
                print("WARNING [{}, {}]: different instruments: "
                      "{} != {} != {}\n\t{}".format(
                          trendall_id, side_id,
                          p, pl, pa, inst_info,
                          ))
            elif p[0]:
                print("WARNING [{}, {}]: missing musical scene type."
                      .format(trendall_id, side_id))
            elif p[1] not in mss:
                continue
            else:
                print("WARNING [{}, {}]: indeterminate error: "
                      "{} != {} != {}\n\t{}".format(
                          trendall_id, side_id,
                          p, pl, pa, inst_info,
                          ))

        # figures
        for (fig_type, fig_count) in side_data['figure_count']:
            figure = Figure(
                figure_type=fig_type,
                figure_count=fig_count,
                side=side,
                )
            objects.append(figure)

        return side

    def _get_side_data(self, row, side_id):
        return {
            'scene_type': self._parse_scene_type(
                row['SCENE_TYPE_' + side_id], False),
            'musical_scene_type': self._parse_scene_type(
                row['MUSICAL_SCENE_TYPE_' + side_id], True),
            'catalogue_description': row[
                'SIDE_{}_DETAILS'.format(side_id)],
            # 'CATALOGUE_DESCRIPTION_' + side_id],
            'details': row['SIDE_%s_DETAILS' % side_id],
            'instruments_and_numbers': self._parse_instr_nos(
                row['SIDE_%s_INSTRUMENTS_AND_NUMBERS' % side_id]),
            'performers': self._parse_scene_type(
                row['SIDE_%s_PERFORMERS' % side_id], True,
                ),
            'performer_location': self._parse_scene_type(
                row['SIDE_%s_PERFORMER_LOCATION' % side_id], True,
                ),
            'performer_action': self._parse_scene_type(
                row['SIDE_%s_PERFORMER_ACTION' % side_id], True,
                ),
            # COUNT_FIGURE
            'figure_count': self._parse_instr_nos(
                row['SIDE_%s_NUMBER_OF_FIGURES' % side_id],
                ),
            'composition': row['SIDE_%s_COMPOSITION' % side_id],
        }

    def _parse_scene_type(self, scene_type, with_trailing=False):
        """\
        Splits a scene type field by comma then by parenthesis, flattening
        it into one list.

        >>> adapter = RowAdapter()
        >>> adapter._parse_scene_type('PROCESSION')
        [('PROCESSION', None)]
        >>> adapter._parse_scene_type('PROCESSION (DIONYSIAC), MYTHOLOGICAL')
        [('PROCESSION', None), ('DIONYSIAC', None), ('MYTHOLOGICAL', None)]

        If with_trailing is True, then each item in the list is a tuple
        pair of the item and the trailing parenthesis for that comma-
        separated segment.

        >>> adapter._parse_scene_type('PROCESSION', True)
        [('PROCESSION', None)]
        >>> adapter._parse_scene_type('PROCESSION(AU)', True)
        [('PROCESSION', 'AU')]
        >>> for scene_type in adapter._parse_scene_type(\
                'ATTENDANT (DIONYSIAC)(TYM), ATTENDANT (DIONYSIAC)(CYM)',\
                True,\
                ):
        ...    print(scene_type)
        ('ATTENDANT', 'TYM')
        ('DIONYSIAC', 'TYM')
        ('ATTENDANT', 'CYM')
        ('DIONYSIAC', 'CYM')
        >>> for scene_type in adapter._parse_scene_type(\
                'C(AU)  R(TYM)', \
                True,\
                ):
        ...     print(scene_type)
        ('C', 'AU')
        ('R', 'TYM')

        """
        names = []
        for s_type in scene_type.split(','):
            theme_names = [
                tname.strip(' ()')
                for tname in s_type.split('(')
                ]
            if with_trailing:
                trailing = theme_names.pop() if len(theme_names) > 1 else None
            else:
                trailing = None
            names += [(tname, trailing) for tname in theme_names]
        return names

    def _parse_instr_nos(self, value):
        inos = []
        for ino in value.split(','):
            if not ino or '_' not in ino:
                continue
            count, instr = ino.split('_')
            if count:
                if count[0].isdigit() and not count.isdigit():
                    count, instr = take_digits(count)
                else:
                    try:
                        count = int(count)
                    except:
                        print("int ERROR on '{}'".format(ino))
                        raise
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
