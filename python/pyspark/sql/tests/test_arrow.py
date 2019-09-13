#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import os
import threading
import time
import unittest
import warnings

from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest
from pyspark.util import _exception_message


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class ArrowTests(ReusedSQLTestCase):

    @classmethod
    def setUpClass(cls):
        from datetime import date, datetime
        from decimal import Decimal
        from distutils.version import LooseVersion
        import pyarrow as pa
        super(ArrowTests, cls).setUpClass()
        cls.warnings_lock = threading.Lock()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.spark.conf.set("spark.sql.session.timeZone", tz)
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        # Disable fallback by default to easily detect the failures.
        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
        cls.schema = StructType([
            StructField("1_str_t", StringType(), True),
            StructField("2_int_t", IntegerType(), True),
            StructField("3_long_t", LongType(), True),
            StructField("4_float_t", FloatType(), True),
            StructField("5_double_t", DoubleType(), True),
            StructField("6_decimal_t", DecimalType(38, 18), True),
            StructField("7_date_t", DateType(), True),
            StructField("8_timestamp_t", TimestampType(), True)])
        cls.data = [(u"a", 1, 10, 0.2, 2.0, Decimal("2.0"),
                     date(1969, 1, 1), datetime(1969, 1, 1, 1, 1, 1)),
                    (u"b", 2, 20, 0.4, 4.0, Decimal("4.0"),
                     date(2012, 2, 2), datetime(2012, 2, 2, 2, 2, 2)),
                    (u"c", 3, 30, 0.8, 6.0, Decimal("6.0"),
                     date(2100, 3, 3), datetime(2100, 3, 3, 3, 3, 3))]

        # TODO: remove version check once minimum pyarrow version is 0.10.0
        if LooseVersion("0.10.0") <= LooseVersion(pa.__version__):
            cls.schema.add(StructField("9_binary_t", BinaryType(), True))
            cls.data[0] = cls.data[0] + (bytearray(b"a"),)
            cls.data[1] = cls.data[1] + (bytearray(b"bb"),)
            cls.data[2] = cls.data[2] + (bytearray(b"ccc"),)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        super(ArrowTests, cls).tearDownClass()

    def create_pandas_data_frame(self):
        import pandas as pd
        import numpy as np
        data_dict = {}
        for j, name in enumerate(self.schema.names):
            data_dict[name] = [self.data[i][j] for i in range(len(self.data))]
        # need to convert these to numpy types first
        data_dict["2_int_t"] = np.int32(data_dict["2_int_t"])
        data_dict["4_float_t"] = np.float32(data_dict["4_float_t"])
        return pd.DataFrame(data=data_dict)

    def test_toPandas_fallback_enabled(self):
        import pandas as pd

        with self.sql_conf({"spark.sql.execution.arrow.fallback.enabled": True}):
            schema = StructType([StructField("map", MapType(StringType(), IntegerType()), True)])
            df = self.spark.createDataFrame([({u'a': 1},)], schema=schema)
            with QuietTest(self.sc):
                with self.warnings_lock:
                    with warnings.catch_warnings(record=True) as warns:
                        # we want the warnings to appear even if this test is run from a subclass
                        warnings.simplefilter("always")
                        pdf = df.toPandas()
                        # Catch and check the last UserWarning.
                        user_warns = [
                            warn.message for warn in warns if isinstance(warn.message, UserWarning)]
                        self.assertTrue(len(user_warns) > 0)
                        self.assertTrue(
                            "Attempting non-optimization" in _exception_message(user_warns[-1]))
                        self.assertPandasEqual(pdf, pd.DataFrame({u'map': [{u'a': 1}]}))

    def test_toPandas_fallback_disabled(self):
        from distutils.version import LooseVersion
        import pyarrow as pa

        schema = StructType([StructField("map", MapType(StringType(), IntegerType()), True)])
        df = self.spark.createDataFrame([(None,)], schema=schema)
        with QuietTest(self.sc):
            with self.warnings_lock:
                with self.assertRaisesRegexp(Exception, 'Unsupported type'):
                    df.toPandas()

        # TODO: remove BinaryType check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            schema = StructType([StructField("binary", BinaryType(), True)])
            df = self.spark.createDataFrame([(None,)], schema=schema)
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(Exception, 'Unsupported type.*BinaryType'):
                    df.toPandas()

    def test_null_conversion(self):
        df_null = self.spark.createDataFrame([tuple([None for _ in range(len(self.data[0]))])] +
                                             self.data)
        pdf = df_null.toPandas()
        null_counts = pdf.isnull().sum().tolist()
        self.assertTrue(all([c == 1 for c in null_counts]))

    def _toPandas_arrow_toggle(self, df):
        with self.sql_conf({"spark.sql.execution.arrow.enabled": False}):
            pdf = df.toPandas()

        pdf_arrow = df.toPandas()

        return pdf, pdf_arrow

    def test_toPandas_arrow_toggle(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        expected = self.create_pandas_data_frame()
        self.assertPandasEqual(expected, pdf)
        self.assertPandasEqual(expected, pdf_arrow)

    def test_toPandas_respect_session_timezone(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)

        timezone = "America/New_York"
        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": False,
                "spark.sql.session.timeZone": timezone}):
            pdf_la, pdf_arrow_la = self._toPandas_arrow_toggle(df)
            self.assertPandasEqual(pdf_arrow_la, pdf_la)

        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": True,
                "spark.sql.session.timeZone": timezone}):
            pdf_ny, pdf_arrow_ny = self._toPandas_arrow_toggle(df)
            self.assertPandasEqual(pdf_arrow_ny, pdf_ny)

            self.assertFalse(pdf_ny.equals(pdf_la))

            from pyspark.sql.types import _check_series_convert_timestamps_local_tz
            pdf_la_corrected = pdf_la.copy()
            for field in self.schema:
                if isinstance(field.dataType, TimestampType):
                    pdf_la_corrected[field.name] = _check_series_convert_timestamps_local_tz(
                        pdf_la_corrected[field.name], timezone)
            self.assertPandasEqual(pdf_ny, pdf_la_corrected)

    def test_pandas_round_trip(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf_arrow = df.toPandas()
        self.assertPandasEqual(pdf_arrow, pdf)

    def test_filtered_frame(self):
        df = self.spark.range(3).toDF("i")
        pdf = df.filter("i < 0").toPandas()
        self.assertEqual(len(pdf.columns), 1)
        self.assertEqual(pdf.columns[0], "i")
        self.assertTrue(pdf.empty)

    def _createDataFrame_toggle(self, pdf, schema=None):
        with self.sql_conf({"spark.sql.execution.arrow.enabled": False}):
            df_no_arrow = self.spark.createDataFrame(pdf, schema=schema)

        df_arrow = self.spark.createDataFrame(pdf, schema=schema)

        return df_no_arrow, df_arrow

    def test_createDataFrame_toggle(self):
        pdf = self.create_pandas_data_frame()
        df_no_arrow, df_arrow = self._createDataFrame_toggle(pdf, schema=self.schema)
        self.assertEquals(df_no_arrow.collect(), df_arrow.collect())

    def test_createDataFrame_respect_session_timezone(self):
        from datetime import timedelta
        pdf = self.create_pandas_data_frame()
        timezone = "America/New_York"
        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": False,
                "spark.sql.session.timeZone": timezone}):
            df_no_arrow_la, df_arrow_la = self._createDataFrame_toggle(pdf, schema=self.schema)
            result_la = df_no_arrow_la.collect()
            result_arrow_la = df_arrow_la.collect()
            self.assertEqual(result_la, result_arrow_la)

        with self.sql_conf({
                "spark.sql.execution.pandas.respectSessionTimeZone": True,
                "spark.sql.session.timeZone": timezone}):
            df_no_arrow_ny, df_arrow_ny = self._createDataFrame_toggle(pdf, schema=self.schema)
            result_ny = df_no_arrow_ny.collect()
            result_arrow_ny = df_arrow_ny.collect()
            self.assertEqual(result_ny, result_arrow_ny)

            self.assertNotEqual(result_ny, result_la)

            # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
            result_la_corrected = [Row(**{k: v - timedelta(hours=3) if k == '8_timestamp_t' else v
                                          for k, v in row.asDict().items()})
                                   for row in result_la]
            self.assertEqual(result_ny, result_la_corrected)

    def test_createDataFrame_with_schema(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertEquals(self.schema, df.schema)
        pdf_arrow = df.toPandas()
        self.assertPandasEqual(pdf_arrow, pdf)

    def test_createDataFrame_with_incorrect_schema(self):
        pdf = self.create_pandas_data_frame()
        fields = list(self.schema)
        fields[0], fields[7] = fields[7], fields[0]  # swap str with timestamp
        wrong_schema = StructType(fields)
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, ".*No cast.*string.*timestamp.*"):
                self.spark.createDataFrame(pdf, schema=wrong_schema)

    def test_createDataFrame_with_names(self):
        pdf = self.create_pandas_data_frame()
        new_names = list(map(str, range(len(self.schema.fieldNames()))))
        # Test that schema as a list of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=list(new_names))
        self.assertEquals(df.schema.fieldNames(), new_names)
        # Test that schema as tuple of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=tuple(new_names))
        self.assertEquals(df.schema.fieldNames(), new_names)

    def test_createDataFrame_column_name_encoding(self):
        import pandas as pd
        pdf = pd.DataFrame({u'a': [1]})
        columns = self.spark.createDataFrame(pdf).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEquals(columns[0], 'a')
        columns = self.spark.createDataFrame(pdf, [u'b']).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEquals(columns[0], 'b')

    def test_createDataFrame_with_single_data_type(self):
        import pandas as pd
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(ValueError, ".*IntegerType.*not supported.*"):
                self.spark.createDataFrame(pd.DataFrame({"a": [1]}), schema="int")

    def test_createDataFrame_does_not_modify_input(self):
        import pandas as pd
        # Some series get converted for Spark to consume, this makes sure input is unchanged
        pdf = self.create_pandas_data_frame()
        # Use a nanosecond value to make sure it is not truncated
        pdf.ix[0, '8_timestamp_t'] = pd.Timestamp(1)
        # Integers with nulls will get NaNs filled with 0 and will be casted
        pdf.ix[1, '2_int_t'] = None
        pdf_copy = pdf.copy(deep=True)
        self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertTrue(pdf.equals(pdf_copy))

    def test_schema_conversion_roundtrip(self):
        from pyspark.sql.types import from_arrow_schema, to_arrow_schema
        arrow_schema = to_arrow_schema(self.schema)
        schema_rt = from_arrow_schema(arrow_schema)
        self.assertEquals(self.schema, schema_rt)

    def test_createDataFrame_with_array_type(self):
        import pandas as pd
        pdf = pd.DataFrame({"a": [[1, 2], [3, 4]], "b": [[u"x", u"y"], [u"y", u"z"]]})
        df, df_arrow = self._createDataFrame_toggle(pdf)
        result = df.collect()
        result_arrow = df_arrow.collect()
        expected = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result_arrow[r][e] and
                                result[r][e] == result_arrow[r][e])

    def test_toPandas_with_array_type(self):
        expected = [([1, 2], [u"x", u"y"]), ([3, 4], [u"y", u"z"])]
        array_schema = StructType([StructField("a", ArrayType(IntegerType())),
                                   StructField("b", ArrayType(StringType()))])
        df = self.spark.createDataFrame(expected, schema=array_schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        result = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        result_arrow = [tuple(list(e) for e in rec) for rec in pdf_arrow.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result_arrow[r][e] and
                                result[r][e] == result_arrow[r][e])

    def test_createDataFrame_with_int_col_names(self):
        import numpy as np
        import pandas as pd
        pdf = pd.DataFrame(np.random.rand(4, 2))
        df, df_arrow = self._createDataFrame_toggle(pdf)
        pdf_col_names = [str(c) for c in pdf.columns]
        self.assertEqual(pdf_col_names, df.columns)
        self.assertEqual(pdf_col_names, df_arrow.columns)

    def test_createDataFrame_fallback_enabled(self):
        import pandas as pd

        with QuietTest(self.sc):
            with self.sql_conf({"spark.sql.execution.arrow.fallback.enabled": True}):
                with warnings.catch_warnings(record=True) as warns:
                    # we want the warnings to appear even if this test is run from a subclass
                    warnings.simplefilter("always")
                    df = self.spark.createDataFrame(
                        pd.DataFrame([[{u'a': 1}]]), "a: map<string, int>")
                    # Catch and check the last UserWarning.
                    user_warns = [
                        warn.message for warn in warns if isinstance(warn.message, UserWarning)]
                    self.assertTrue(len(user_warns) > 0)
                    self.assertTrue(
                        "Attempting non-optimization" in _exception_message(user_warns[-1]))
                    self.assertEqual(df.collect(), [Row(a={u'a': 1})])

    def test_createDataFrame_fallback_disabled(self):
        from distutils.version import LooseVersion
        import pandas as pd
        import pyarrow as pa

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(TypeError, 'Unsupported type'):
                self.spark.createDataFrame(
                    pd.DataFrame([[{u'a': 1}]]), "a: map<string, int>")

        # TODO: remove BinaryType check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(TypeError, 'Unsupported type.*BinaryType'):
                    self.spark.createDataFrame(
                        pd.DataFrame([[{'a': b'aaa'}]]), "a: binary")

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        import pandas as pd
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime.datetime(2015, 11, 1, 0, 30),
              datetime.datetime(2015, 11, 1, 1, 30),
              datetime.datetime(2015, 11, 1, 2, 30)]
        pdf = pd.DataFrame({'time': dt})

        df_from_python = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        df_from_pandas = self.spark.createDataFrame(pdf)

        self.assertPandasEqual(pdf, df_from_python.toPandas())
        self.assertPandasEqual(pdf, df_from_pandas.toPandas())

    def test_toPandas_batch_order(self):

        def delay_first_part(partition_index, iterator):
            if partition_index == 0:
                time.sleep(0.1)
            return iterator

        # Collects Arrow RecordBatches out of order in driver JVM then re-orders in Python
        def run_test(num_records, num_parts, max_records, use_delay=False):
            df = self.spark.range(num_records, numPartitions=num_parts).toDF("a")
            if use_delay:
                df = df.rdd.mapPartitionsWithIndex(delay_first_part).toDF()
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": max_records}):
                pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
                self.assertPandasEqual(pdf, pdf_arrow)

        cases = [
            (1024, 512, 2),    # Use large num partitions for more likely collecting out of order
            (64, 8, 2, True),  # Use delay in first partition to force collecting out of order
            (64, 64, 1),       # Test single batch per partition
            (64, 1, 64),       # Test single partition, single batch
            (64, 1, 8),        # Test single partition, multiple batches
            (30, 7, 2),        # Test different sized partitions
        ]

        for case in cases:
            run_test(*case)


class EncryptionArrowTests(ArrowTests):

    @classmethod
    def conf(cls):
        return super(EncryptionArrowTests, cls).conf().set("spark.io.encryption.enabled", "true")


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
