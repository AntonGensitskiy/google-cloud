/*
 * Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.plugin.gcp.bigquery.util;

/**
 * BigQuery constants.
 */
public interface BigQueryConstants {

  String CONFIG_ALLOW_SCHEMA_RELAXATION = "mapred.bg.sink.allow_schema_relaxation";
  String CONFIG_ALLOW_TIME_PARTITIONING = "mapred.bg.sink.allow_time_partitioning";
  String CONFIG_PARTITION_BY_FIELD = "mapred.bg.sink.partition_by_field";
  String CONFIG_REQUIRE_PARTITION_FILTER = "mapred.bg.sink.require_partition_filter";
}
