#!/usr/bin/env python3

"""Run doc tests on the apulian modules."""


import doctest

import apulian.adapter
import apulian.models
import apulian.utils


if __name__ == '__main__':
    for m in [apulian.adapter, apulian.models, apulian.utils]:
        doctest.testmod(m)
