/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.TableProvider;
import org.apache.spark.sql.sources.v2.reader.*;

public class JavaSimpleDataSourceV2 implements TableProvider {

  class MyScanBuilder extends JavaSimpleScanBuilder {

    @Override
    public InputPartition[] planInputPartitions() {
      InputPartition[] partitions = new InputPartition[2];
      partitions[0] = new JavaRangeInputPartition(0, 5);
      partitions[1] = new JavaRangeInputPartition(5, 10);
      return partitions;
    }
  }

  @Override
  public Table getTable(DataSourceOptions options) {
    return new JavaSimpleBatchTable() {
      @Override
      public ScanBuilder newScanBuilder(DataSourceOptions options) {
        return new MyScanBuilder();
      }
    };
  }
}
