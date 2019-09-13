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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.sources.v2.reader.Scan;
import org.apache.spark.sql.sources.v2.reader.ScanBuilder;

/**
 * An internal base interface of mix-in interfaces for readable {@link Table}. This adds
 * {@link #newScanBuilder(DataSourceOptions)} that is used to create a scan for batch, micro-batch,
 * or continuous processing.
 */
interface SupportsRead extends Table {

  /**
   * Returns a {@link ScanBuilder} which can be used to build a {@link Scan}. Spark will call this
   * method to configure each scan.
   */
  ScanBuilder newScanBuilder(DataSourceOptions options);
}
