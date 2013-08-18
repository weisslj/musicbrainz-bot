#!/usr/bin/env python
import doctest

doctest.testfile('tests/split_artists.txt', optionflags=doctest.REPORT_CDIFF)

