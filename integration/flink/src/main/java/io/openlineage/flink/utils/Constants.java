/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

public class Constants {
  public static String TABLE_TYPE = "Table";
  public static String KAFKA_TYPE = "Kafka";
  public static String CASSANDRA_TYPE = "Cassandra";
  public static String HADOOP_CATALOG_NAME = "Hadoop";
  public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
  public static String INJECTED_BY_KAFFE = "injected_by_kaffe";

  public static final String KAFKA_TOPIC_DESCRIPTOR_CLASS =
      "org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor";
  public static final String KAFKA_PARTITION_DISCOVERER_CLASS =
      "org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer";
  public static final String AVRO_DESERIALIZATION_SCHEMA_CLASS =
      "org.apache.flink.formats.avro.AvroDeserializationSchema";
  public static final String VALUE_ONLY_DESERIALIZATION_SCHEMA_WRAPPER_CLASS =
      "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaValueOnlyDeserializationSchemaWrapper";
  public static final String DESERIALIZATION_SCHEMA_WRAPPER_CLASS =
      "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaDeserializationSchemaWrapper";
  public static final String DYNAMIC_DESERIALIZATION_SCHEMA_CLASS =
      "org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaDeserializationSchema";

  public static final String JDBC_OUTPUT_FORMAT_CLASS =
      "org.apache.flink.connector.jdbc.internal.JdbcOutputFormat";
  public static final String GENERIC_JDBC_SINK_FUNCTION_CLASS =
      "org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction";
  public static final String JDBC_XA_FINK_FUNCTION_CLASS =
      "org.apache.flink.connector.jdbc.xa.JdbcXaSinkFunction";
  public static final String JDBC_CONNECTION_OPTIONS_CLASS =
      "org.apache.flink.connector.jdbc.JdbcConnectionOptions";
  public static final String INTERNAL_JDBC_CONNECTION_OPTIONS_CLASS =
      "org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions";

  public static final String SIMPLE_JDBC_CONNECTION_PROVIDER_CLASS =
      "org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider";

  public static final String JDBC_INPUT_FORMAT_CLASS =
      "org.apache.flink.connector.jdbc.JdbcInputFormat";
  public static final String JDBC_ROW_DATA_INPUT_FORMAT_CLASS =
      "org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat";
  public static final String JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS =
      "org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction";
}
