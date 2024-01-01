/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

@Slf4j
public class FlinkKafkaConsumerWrapper {

  private final Object flinkKafkaConsumer;
  private final ClassLoader userClassLoader;

  private FlinkKafkaConsumerWrapper(Object flinkKafkaConsumer, ClassLoader userClassLoader) {
    this.flinkKafkaConsumer = flinkKafkaConsumer;
    this.userClassLoader = userClassLoader;
  }

  public static FlinkKafkaConsumerWrapper of(
      Object flinkKafkaConsumer, ClassLoader userClassLoader) {
    return new FlinkKafkaConsumerWrapper(flinkKafkaConsumer, userClassLoader);
  }

  public Properties getKafkaProperties() {
    return getField("properties");
  }

  public List<String> getTopics() throws IllegalAccessException {
    KafkaTopicsDescriptor descriptor = getField("topicsDescriptor");
    if (descriptor.isFixedTopics()) {
      return descriptor.getFixedTopics();
    }

    // TODO: this will call Kafka. It's not clear whether we always can use it.
    Properties kafkaProperties = getField("properties");

    KafkaPartitionDiscoverer partitionDiscoverer =
        new KafkaPartitionDiscoverer(descriptor, 0, 0, kafkaProperties);
    WrapperUtils.<List<String>>invoke(
        KafkaPartitionDiscoverer.class, partitionDiscoverer, "initializeConnections");
    return WrapperUtils.<List<String>>invoke(
            KafkaPartitionDiscoverer.class, partitionDiscoverer, "getAllTopics")
        .get();
  }

  public KafkaDeserializationSchema getDeserializationSchema() throws IllegalAccessException {
    return getField("deserializer");
  }

  public Optional<Schema> getAvroSchema() {
    Optional<Class> kafkaDeserializationSchemaWrapperClass =
        getKafkaDeserializationSchemaWrapperClass();

    if (kafkaDeserializationSchemaWrapperClass.isEmpty()) {
      log.error("Cannot extract Avro schema: KafkaDeserializationSchemaWrapper not found");
      return Optional.empty();
    }

    try {
      return Optional.of(getDeserializationSchema())
          .filter(
              el -> el.getClass().isAssignableFrom(kafkaDeserializationSchemaWrapperClass.get()))
          .flatMap(
              el ->
                  WrapperUtils.<DeserializationSchema>getFieldValue(
                      el.getClass(), el, "deserializationSchema"))
          .filter(schema -> schema instanceof AvroDeserializationSchema)
          .map(schema -> (AvroDeserializationSchema) schema)
          .map(schema -> schema.getProducedType())
          .flatMap(typeInformation -> Optional.ofNullable(typeInformation.getTypeClass()))
          .flatMap(aClass -> WrapperUtils.<Schema>invokeStatic(aClass, "getClassSchema"));
    } catch (IllegalAccessException e) {
      log.error("Cannot extract Avro schema: ", e);
      return Optional.empty();
    }
  }

  Optional<Class> getKafkaDeserializationSchemaWrapperClass() {
    try {
      return Optional.of(
          userClassLoader.loadClass(
              "org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchemaWrapper"));
    } catch (ClassNotFoundException e) {
      // do nothing - give another try
    }

    try {
      // class renamed in newer Flink versions
      return Optional.of(
          userClassLoader.loadClass(
              "org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper"));
    } catch (ClassNotFoundException e) {

    }

    log.error("Couldn't find KafkaDeserializationSchemaWrapper class");
    return Optional.empty();
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(flinkKafkaConsumer.getClass(), flinkKafkaConsumer, name)
        .get();
  }
}
