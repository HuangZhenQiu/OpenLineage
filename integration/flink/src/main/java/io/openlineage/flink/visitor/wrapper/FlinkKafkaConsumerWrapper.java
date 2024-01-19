/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.Constants.AVRO_DESERIALIZATION_SCHEMA_CLASS;

import io.openlineage.flink.utils.KafkaUtils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

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
    try {
      Object descriptor = getField("topicsDescriptor");
      if (WrapperUtils.<Boolean>invoke(descriptor.getClass(), descriptor, "isFixedTopics").get()) {
        log.debug("Getting all topics from fixed topics");
        return WrapperUtils.<List<String>>invoke(
                descriptor.getClass(), descriptor, "getFixedTopics")
            .get();
      }

      Optional<Pattern> topicPattern =
          WrapperUtils.<Pattern>getFieldValue(descriptor.getClass(), descriptor, "topicPattern");

      if (topicPattern.isPresent()) {
        log.debug("Topic pattern is {}", topicPattern.get());
        List<String> topics =
            KafkaUtils.getAllTopics(userClassLoader, descriptor, getField("properties"));
        // Need to additional filter here
        return topics.stream()
            .filter(
                topic -> {
                  log.debug(
                      "test pattern match {} result is {}",
                      topic,
                      topicPattern.get().matcher(topic).find());
                  return topicPattern.get().matcher(topic).find();
                })
            .collect(Collectors.toList());
      }
    } catch (Exception e) {
      log.error("Cannot get all topics from topic pattern ", e);
    }

    return List.of();
  }

  public Object getDeserializationSchema() throws IllegalAccessException {
    return getField("deserializer");
  }

  public Optional<Schema> getAvroSchema() {
    Optional<Class> kafkaDeserializationSchemaWrapperClass =
        getKafkaDeserializationSchemaWrapperClass();

    if (kafkaDeserializationSchemaWrapperClass.isEmpty()) {
      log.debug("Cannot extract Avro schema: KafkaDeserializationSchemaWrapper not found");
      return Optional.empty();
    }

    try {
      final Class avroDeserializationSchemaClass =
          userClassLoader.loadClass(AVRO_DESERIALIZATION_SCHEMA_CLASS);

      return Optional.of(getDeserializationSchema())
          .filter(
              el -> el.getClass().isAssignableFrom(kafkaDeserializationSchemaWrapperClass.get()))
          .flatMap(
              el -> WrapperUtils.<Object>getFieldValue(el.getClass(), el, "deserializationSchema"))
          .filter(schema -> avroDeserializationSchemaClass.isAssignableFrom(schema.getClass()))
          .map(
              schema ->
                  WrapperUtils.<TypeInformation>invoke(
                          avroDeserializationSchemaClass, schema, "getProducedType")
                      .get())
          .flatMap(typeInformation -> Optional.ofNullable(typeInformation.getTypeClass()))
          .flatMap(aClass -> WrapperUtils.<Schema>invokeStatic(aClass, "getClassSchema"));
    } catch (IllegalAccessException | ClassNotFoundException e) {
      log.debug("Cannot extract Avro schema: ", e);
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
