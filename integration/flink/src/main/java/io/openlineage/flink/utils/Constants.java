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
}
