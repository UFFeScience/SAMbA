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
import unittest

from collections import OrderedDict
from decimal import Decimal
from distutils.version import LooseVersion

from pyspark.sql import Row
from pyspark.sql.functions import array, explode, col, lit, udf, sum, pandas_udf, PandasUDFType
from pyspark.sql.types import *
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class GroupedMapPandasUDFTests(ReusedSQLTestCase):

    @property
    def data(self):
        return self.spark.range(10).toDF('id') \
            .withColumn("vs", array([lit(i) for i in range(20, 30)])) \
            .withColumn("v", explode(col('vs'))).drop('vs')

    def test_supported_types(self):
        import pyarrow as pa

        values = [
            1, 2, 3,
            4, 5, 1.1,
            2.2, Decimal(1.123),
            [1, 2, 2], True, 'hello'
        ]
        output_fields = [
            ('id', IntegerType()), ('byte', ByteType()), ('short', ShortType()),
            ('int', IntegerType()), ('long', LongType()), ('float', FloatType()),
            ('double', DoubleType()), ('decim', DecimalType(10, 3)),
            ('array', ArrayType(IntegerType())), ('bool', BooleanType()), ('str', StringType())
        ]

        # TODO: Add BinaryType to variables above once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) >= LooseVersion("0.10.0"):
            values.append(bytearray([0x01, 0x02]))
            output_fields.append(('bin', BinaryType()))

        output_schema = StructType([StructField(*x) for x in output_fields])
        df = self.spark.createDataFrame([values], schema=output_schema)

        # Different forms of group map pandas UDF, results of these are the same
        udf1 = pandas_udf(
            lambda pdf: pdf.assign(
                byte=pdf.byte * 2,
                short=pdf.short * 2,
                int=pdf.int * 2,
                long=pdf.long * 2,
                float=pdf.float * 2,
                double=pdf.double * 2,
                decim=pdf.decim * 2,
                bool=False if pdf.bool else True,
                str=pdf.str + 'there',
                array=pdf.array,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP
        )

        udf2 = pandas_udf(
            lambda _, pdf: pdf.assign(
                byte=pdf.byte * 2,
                short=pdf.short * 2,
                int=pdf.int * 2,
                long=pdf.long * 2,
                float=pdf.float * 2,
                double=pdf.double * 2,
                decim=pdf.decim * 2,
                bool=False if pdf.bool else True,
                str=pdf.str + 'there',
                array=pdf.array,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP
        )

        udf3 = pandas_udf(
            lambda key, pdf: pdf.assign(
                id=key[0],
                byte=pdf.byte * 2,
                short=pdf.short * 2,
                int=pdf.int * 2,
                long=pdf.long * 2,
                float=pdf.float * 2,
                double=pdf.double * 2,
                decim=pdf.decim * 2,
                bool=False if pdf.bool else True,
                str=pdf.str + 'there',
                array=pdf.array,
            ),
            output_schema,
            PandasUDFType.GROUPED_MAP
        )

        result1 = df.groupby('id').apply(udf1).sort('id').toPandas()
        expected1 = df.toPandas().groupby('id').apply(udf1.func).reset_index(drop=True)

        result2 = df.groupby('id').apply(udf2).sort('id').toPandas()
        expected2 = expected1

        result3 = df.groupby('id').apply(udf3).sort('id').toPandas()
        expected3 = expected1

        self.assertPandasEqual(expected1, result1)
        self.assertPandasEqual(expected2, result2)
        self.assertPandasEqual(expected3, result3)

    def test_array_type_correct(self):
        df = self.data.withColumn("arr", array(col("id"))).repartition(1, "id")

        output_schema = StructType(
            [StructField('id', LongType()),
             StructField('v', IntegerType()),
             StructField('arr', ArrayType(LongType()))])

        udf = pandas_udf(
            lambda pdf: pdf,
            output_schema,
            PandasUDFType.GROUPED_MAP
        )

        result = df.groupby('id').apply(udf).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(udf.func).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

    def test_register_grouped_map_udf(self):
        foo_udf = pandas_udf(lambda x: x, "id long", PandasUDFType.GROUPED_MAP)
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    ValueError,
                    'f.*SQL_BATCHED_UDF.*SQL_SCALAR_PANDAS_UDF.*SQL_GROUPED_AGG_PANDAS_UDF.*'):
                self.spark.catalog.registerFunction("foo_udf", foo_udf)

    def test_decorator(self):
        df = self.data

        @pandas_udf(
            'id long, v int, v1 double, v2 long',
            PandasUDFType.GROUPED_MAP
        )
        def foo(pdf):
            return pdf.assign(v1=pdf.v * pdf.id * 1.0, v2=pdf.v + pdf.id)

        result = df.groupby('id').apply(foo).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(foo.func).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

    def test_coerce(self):
        df = self.data

        foo = pandas_udf(
            lambda pdf: pdf,
            'id long, v double',
            PandasUDFType.GROUPED_MAP
        )

        result = df.groupby('id').apply(foo).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(foo.func).reset_index(drop=True)
        expected = expected.assign(v=expected.v.astype('float64'))
        self.assertPandasEqual(expected, result)

    def test_complex_groupby(self):
        df = self.data

        @pandas_udf(
            'id long, v int, norm double',
            PandasUDFType.GROUPED_MAP
        )
        def normalize(pdf):
            v = pdf.v
            return pdf.assign(norm=(v - v.mean()) / v.std())

        result = df.groupby(col('id') % 2 == 0).apply(normalize).sort('id', 'v').toPandas()
        pdf = df.toPandas()
        expected = pdf.groupby(pdf['id'] % 2 == 0).apply(normalize.func)
        expected = expected.sort_values(['id', 'v']).reset_index(drop=True)
        expected = expected.assign(norm=expected.norm.astype('float64'))
        self.assertPandasEqual(expected, result)

    def test_empty_groupby(self):
        df = self.data

        @pandas_udf(
            'id long, v int, norm double',
            PandasUDFType.GROUPED_MAP
        )
        def normalize(pdf):
            v = pdf.v
            return pdf.assign(norm=(v - v.mean()) / v.std())

        result = df.groupby().apply(normalize).sort('id', 'v').toPandas()
        pdf = df.toPandas()
        expected = normalize.func(pdf)
        expected = expected.sort_values(['id', 'v']).reset_index(drop=True)
        expected = expected.assign(norm=expected.norm.astype('float64'))
        self.assertPandasEqual(expected, result)

    def test_datatype_string(self):
        df = self.data

        foo_udf = pandas_udf(
            lambda pdf: pdf.assign(v1=pdf.v * pdf.id * 1.0, v2=pdf.v + pdf.id),
            'id long, v int, v1 double, v2 long',
            PandasUDFType.GROUPED_MAP
        )

        result = df.groupby('id').apply(foo_udf).sort('id').toPandas()
        expected = df.toPandas().groupby('id').apply(foo_udf.func).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

    def test_wrong_return_type(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid returnType.*grouped map Pandas UDF.*MapType'):
                pandas_udf(
                    lambda pdf: pdf,
                    'id long, v map<int, int>',
                    PandasUDFType.GROUPED_MAP)

    def test_wrong_args(self):
        df = self.data

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(lambda x: x)
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(udf(lambda x: x, DoubleType()))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(sum(df.v))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(df.v + 1)
            with self.assertRaisesRegexp(ValueError, 'Invalid function'):
                df.groupby('id').apply(
                    pandas_udf(lambda: 1, StructType([StructField("d", DoubleType())])))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf'):
                df.groupby('id').apply(pandas_udf(lambda x, y: x, DoubleType()))
            with self.assertRaisesRegexp(ValueError, 'Invalid udf.*GROUPED_MAP'):
                df.groupby('id').apply(
                    pandas_udf(lambda x, y: x, DoubleType(), PandasUDFType.SCALAR))

    def test_unsupported_types(self):
        import pyarrow as pa

        common_err_msg = 'Invalid returnType.*grouped map Pandas UDF.*'
        unsupported_types = [
            StructField('map', MapType(StringType(), IntegerType())),
            StructField('arr_ts', ArrayType(TimestampType())),
            StructField('null', NullType()),
        ]

        # TODO: Remove this if-statement once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            unsupported_types.append(StructField('bin', BinaryType()))

        for unsupported_type in unsupported_types:
            schema = StructType([StructField('id', LongType(), True), unsupported_type])
            with QuietTest(self.sc):
                with self.assertRaisesRegexp(NotImplementedError, common_err_msg):
                    pandas_udf(lambda x: x, schema, PandasUDFType.GROUPED_MAP)

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime.datetime(2015, 11, 1, 0, 30),
              datetime.datetime(2015, 11, 1, 1, 30),
              datetime.datetime(2015, 11, 1, 2, 30)]
        df = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        foo_udf = pandas_udf(lambda pdf: pdf, 'time timestamp', PandasUDFType.GROUPED_MAP)
        result = df.groupby('time').apply(foo_udf).sort('time')
        self.assertPandasEqual(df.toPandas(), result.toPandas())

    def test_udf_with_key(self):
        import numpy as np

        df = self.data
        pdf = df.toPandas()

        def foo1(key, pdf):
            assert type(key) == tuple
            assert type(key[0]) == np.int64

            return pdf.assign(v1=key[0],
                              v2=pdf.v * key[0],
                              v3=pdf.v * pdf.id,
                              v4=pdf.v * pdf.id.mean())

        def foo2(key, pdf):
            assert type(key) == tuple
            assert type(key[0]) == np.int64
            assert type(key[1]) == np.int32

            return pdf.assign(v1=key[0],
                              v2=key[1],
                              v3=pdf.v * key[0],
                              v4=pdf.v + key[1])

        def foo3(key, pdf):
            assert type(key) == tuple
            assert len(key) == 0
            return pdf.assign(v1=pdf.v * pdf.id)

        # v2 is int because numpy.int64 * pd.Series<int32> results in pd.Series<int32>
        # v3 is long because pd.Series<int64> * pd.Series<int32> results in pd.Series<int64>
        udf1 = pandas_udf(
            foo1,
            'id long, v int, v1 long, v2 int, v3 long, v4 double',
            PandasUDFType.GROUPED_MAP)

        udf2 = pandas_udf(
            foo2,
            'id long, v int, v1 long, v2 int, v3 int, v4 int',
            PandasUDFType.GROUPED_MAP)

        udf3 = pandas_udf(
            foo3,
            'id long, v int, v1 long',
            PandasUDFType.GROUPED_MAP)

        # Test groupby column
        result1 = df.groupby('id').apply(udf1).sort('id', 'v').toPandas()
        expected1 = pdf.groupby('id')\
            .apply(lambda x: udf1.func((x.id.iloc[0],), x))\
            .sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected1, result1)

        # Test groupby expression
        result2 = df.groupby(df.id % 2).apply(udf1).sort('id', 'v').toPandas()
        expected2 = pdf.groupby(pdf.id % 2)\
            .apply(lambda x: udf1.func((x.id.iloc[0] % 2,), x))\
            .sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected2, result2)

        # Test complex groupby
        result3 = df.groupby(df.id, df.v % 2).apply(udf2).sort('id', 'v').toPandas()
        expected3 = pdf.groupby([pdf.id, pdf.v % 2])\
            .apply(lambda x: udf2.func((x.id.iloc[0], (x.v % 2).iloc[0],), x))\
            .sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected3, result3)

        # Test empty groupby
        result4 = df.groupby().apply(udf3).sort('id', 'v').toPandas()
        expected4 = udf3.func((), pdf)
        self.assertPandasEqual(expected4, result4)

    def test_column_order(self):
        import pandas as pd

        # Helper function to set column names from a list
        def rename_pdf(pdf, names):
            pdf.rename(columns={old: new for old, new in
                                zip(pd_result.columns, names)}, inplace=True)

        df = self.data
        grouped_df = df.groupby('id')
        grouped_pdf = df.toPandas().groupby('id')

        # Function returns a pdf with required column names, but order could be arbitrary using dict
        def change_col_order(pdf):
            # Constructing a DataFrame from a dict should result in the same order,
            # but use from_items to ensure the pdf column order is different than schema
            return pd.DataFrame.from_items([
                ('id', pdf.id),
                ('u', pdf.v * 2),
                ('v', pdf.v)])

        ordered_udf = pandas_udf(
            change_col_order,
            'id long, v int, u int',
            PandasUDFType.GROUPED_MAP
        )

        # The UDF result should assign columns by name from the pdf
        result = grouped_df.apply(ordered_udf).sort('id', 'v')\
            .select('id', 'u', 'v').toPandas()
        pd_result = grouped_pdf.apply(change_col_order)
        expected = pd_result.sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

        # Function returns a pdf with positional columns, indexed by range
        def range_col_order(pdf):
            # Create a DataFrame with positional columns, fix types to long
            return pd.DataFrame(list(zip(pdf.id, pdf.v * 3, pdf.v)), dtype='int64')

        range_udf = pandas_udf(
            range_col_order,
            'id long, u long, v long',
            PandasUDFType.GROUPED_MAP
        )

        # The UDF result uses positional columns from the pdf
        result = grouped_df.apply(range_udf).sort('id', 'v') \
            .select('id', 'u', 'v').toPandas()
        pd_result = grouped_pdf.apply(range_col_order)
        rename_pdf(pd_result, ['id', 'u', 'v'])
        expected = pd_result.sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

        # Function returns a pdf with columns indexed with integers
        def int_index(pdf):
            return pd.DataFrame(OrderedDict([(0, pdf.id), (1, pdf.v * 4), (2, pdf.v)]))

        int_index_udf = pandas_udf(
            int_index,
            'id long, u int, v int',
            PandasUDFType.GROUPED_MAP
        )

        # The UDF result should assign columns by position of integer index
        result = grouped_df.apply(int_index_udf).sort('id', 'v') \
            .select('id', 'u', 'v').toPandas()
        pd_result = grouped_pdf.apply(int_index)
        rename_pdf(pd_result, ['id', 'u', 'v'])
        expected = pd_result.sort_values(['id', 'v']).reset_index(drop=True)
        self.assertPandasEqual(expected, result)

        @pandas_udf('id long, v int', PandasUDFType.GROUPED_MAP)
        def column_name_typo(pdf):
            return pd.DataFrame({'iid': pdf.id, 'v': pdf.v})

        @pandas_udf('id long, v int', PandasUDFType.GROUPED_MAP)
        def invalid_positional_types(pdf):
            return pd.DataFrame([(u'a', 1.2)])

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(Exception, "KeyError: 'id'"):
                grouped_df.apply(column_name_typo).collect()
            import pyarrow as pa
            if LooseVersion(pa.__version__) < LooseVersion("0.11.0"):
                # TODO: see ARROW-1949. Remove when the minimum PyArrow version becomes 0.11.0.
                with self.assertRaisesRegexp(Exception, "No cast implemented"):
                    grouped_df.apply(invalid_positional_types).collect()
            else:
                with self.assertRaisesRegexp(Exception, "an integer is required"):
                    grouped_df.apply(invalid_positional_types).collect()

    def test_positional_assignment_conf(self):
        import pandas as pd

        with self.sql_conf({
                "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}):

            @pandas_udf("a string, b float", PandasUDFType.GROUPED_MAP)
            def foo(_):
                return pd.DataFrame([('hi', 1)], columns=['x', 'y'])

            df = self.data
            result = df.groupBy('id').apply(foo).select('a', 'b').collect()
            for r in result:
                self.assertEqual(r.a, 'hi')
                self.assertEqual(r.b, 1)

    def test_self_join_with_pandas(self):
        @pandas_udf('key long, col string', PandasUDFType.GROUPED_MAP)
        def dummy_pandas_udf(df):
            return df[['key', 'col']]

        df = self.spark.createDataFrame([Row(key=1, col='A'), Row(key=1, col='B'),
                                         Row(key=2, col='C')])
        df_with_pandas = df.groupBy('key').apply(dummy_pandas_udf)

        # this was throwing an AnalysisException before SPARK-24208
        res = df_with_pandas.alias('temp0').join(df_with_pandas.alias('temp1'),
                                                 col('temp0.key') == col('temp1.key'))
        self.assertEquals(res.count(), 5)

    def test_mixed_scalar_udfs_followed_by_grouby_apply(self):
        import pandas as pd

        df = self.spark.range(0, 10).toDF('v1')
        df = df.withColumn('v2', udf(lambda x: x + 1, 'int')(df['v1'])) \
            .withColumn('v3', pandas_udf(lambda x: x + 2, 'int')(df['v1']))

        result = df.groupby() \
            .apply(pandas_udf(lambda x: pd.DataFrame([x.sum().sum()]),
                              'sum int',
                              PandasUDFType.GROUPED_MAP))

        self.assertEquals(result.collect()[0]['sum'], 165)


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_udf_grouped_map import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
