import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam
from beam_pipeline import  TransformCsv

class TransactionTest(unittest.TestCase):


    def test_transaction_filter(self):
        # Our input data, which will make up the initial PCollection.
        ROWS = [
            "2012-01-01 14:09:16 UTC,wallet1,wallet1a,0",
            "2013-01-01 14:09:16 UTC,wallet2,wallet2a,10",
            "2014-01-01 14:09:16 UTC,wallet2,wallet2a,19",
            "2015-01-01 14:09:16 UTC,wallet3,wallet3a,21",
            "2016-01-01 14:09:16 UTC,wallet3,wallet3a,30"
        ]

        # Our output data, which is the expected data that the final PCollection must match.
        EXPECTED_ROWS = ['2015-01-01,21.0',
                         '2016-01-01,30.0']

        with TestPipeline() as p:

            input = p | beam.Create(ROWS)
            output = input | TransformCsv()

            assert_that(output, equal_to(EXPECTED_ROWS), label='CheckOutput')

    def test_year_filter(self):
        # Our input data, which will make up the initial PCollection.
        ROWS = [
            "2009-01-09 02:54:25 UTC,wallet1,wallet1a,50",
            "2010-01-01 04:22:23 UTC,wallet2,wallet2a,50",
            "2011-01-01 04:22:23 UTC,wallet2,wallet2a,50",
            "2012-01-01 14:09:16 UTC,wallet3,wallet3a,50"
        ]

        # Our output data, which is the expected data that the final PCollection must match.
        EXPECTED_ROWS = ['2011-01-01,50.0',
                         '2012-01-01,50.0']

        with TestPipeline() as p:

            input = p | beam.Create(ROWS)
            output = input | TransformCsv()

            assert_that(output, equal_to(EXPECTED_ROWS), label='CheckOutput')

    def test_data_grouping(self):
        # Our input data, which will make up the initial PCollection.
        ROWS = [
            "2011-01-09 02:54:25 UTC,wallet1,wallet1a,40",
            "2011-01-09 02:54:25 UTC,wallet1,wallet1a,40",
            "2012-01-01 14:09:16 UTC,wallet3,wallet3a,50",
            "2012-01-01 14:09:16 UTC,wallet3,wallet3a,50"
        ]

        # Our output data, which is the expected data that the final PCollection must match.
        EXPECTED_ROWS = ['2011-01-09,80.0',
                         '2012-01-01,100.0']

        with TestPipeline() as p:

            input = p | beam.Create(ROWS)
            output = input | TransformCsv()

            assert_that(output, equal_to(EXPECTED_ROWS), label='CheckOutput')

    def test_GC_file(self):
        # Our input data, which will make up the initial PCollection.
        ROWS = [
            "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
            "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
            "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
            "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030",
            "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
            "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12"
        ]

        # Our output data, which is the expected data that the final PCollection must match.
        EXPECTED_ROWS = ['2017-03-18,2102.22',
                         '2017-08-31,13700000023.08',
                         '2018-02-27,129.12']

        with TestPipeline() as p:

            input = p | beam.Create(ROWS)
            output = input | TransformCsv()

            assert_that(output, equal_to(EXPECTED_ROWS), label='CheckOutput')
