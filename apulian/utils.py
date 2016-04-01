
"""\
Utilities
"""


import csv


def take_digits(inp):
    """\
    This strips of the digits from the beginning of the string and returns as
    an int. The also returns the rest of the input.

    >>> take_digits('abc')
    (None, 'abc')
    >>> take_digits('1abc')
    (1, 'abc')
    >>> take_digits('42xyz')
    (42, 'xyz')
    >>> take_digits('13')
    (13, '')

    """

    accum = []
    rest = inp

    for i in range(len(inp)):
        if inp[i].isdigit():
            accum.append(inp[i])
        else:
            rest = inp[i:]
            break
    else:
        rest = ''

    if not accum:
        num = None
    else:
        num = int(''.join(accum))

    return (num, rest)


def read_csv(filename):
    """This reads the CSV files. """
    with open(filename, encoding='latin1') as fin:
        reader = csv.DictReader(fin)
        for row in reader:
            yield row
