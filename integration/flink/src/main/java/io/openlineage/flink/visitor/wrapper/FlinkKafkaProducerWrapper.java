/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.CommonUtils.isInstanceOf;

import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkKafkaProducerWrapper {
  Object flinkKafkaProducer;

  ClassLoader userClassLoader;

  private FlinkKafkaProducerWrapper(Object flinkKafkaProducer, ClassLoader userClassLoader) {
    this.flinkKafkaProducer = flinkKafkaProducer;
    this.userClassLoader = userClassLoader;
  }

  public static FlinkKafkaProducerWrapper of(
      Object flinkKafkaProducer, ClassLoader userClassLoader) {
    return new FlinkKafkaProducerWrapper(flinkKafkaProducer, userClassLoader);
  }

  public String getKafkaTopic() {
    return getField("defaultTopicId");
  }

  public Properties getKafkaProducerConfig() {
    return getField("producerConfig");
  }

  public Optional<Object> getAvroSchema() {
    Optional<Object> keyedSchema =
        WrapperUtils.getFieldValue(
            flinkKafkaProducer.getClass(), flinkKafkaProducer, "keyedSchema");
    if (keyedSchema.isPresent()) {
      return getKeyedAvroSchema(keyedSchema.get());
    }
    return getKafkaAvroSchema();
  }

  private Optional<Object> getKeyedAvroSchema(Object serializationSchema) {
    if (isInstanceOf(
        userClassLoader,
        serializationSchema,
        "org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper")) {
      return AvroUtils.getAvroSchema(
          userClassLoader,
          WrapperUtils.getFieldValue(
              serializationSchema.getClass(), serializationSchema, "serializationSchema"));
    }

    log.debug("Couldn't find Keyed Avro Schema");
    return Optional.empty();
  }

  private Optional<Object> getKafkaAvroSchema() {
    Object kafkaSchema = getField("kafkaSchema");
    if (isInstanceOf(
        userClassLoader,
        kafkaSchema,
        "org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper")) {
      return AvroUtils.getAvroSchema(
          userClassLoader,
          WrapperUtils.getFieldValue(kafkaSchema.getClass(), kafkaSchema, "serializationSchema"));
    }

    log.debug("Couldn't find Avro Schema");
    return Optional.empty();
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(flinkKafkaProducer.getClass(), flinkKafkaProducer, name)
        .get();
  }
}
