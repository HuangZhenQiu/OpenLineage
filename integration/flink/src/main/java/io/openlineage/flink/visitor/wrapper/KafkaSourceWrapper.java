/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Wrapper class to extract hidden fields and call hidden methods on KafkaSource object. It
 * encapsulates all the reflection methods used on KafkaSource.
 */
@Slf4j
public class KafkaSourceWrapper {

  private static final String DESERIALIZATION_SCHEMA_WRAPPER_CLASS =
      "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaValueOnlyDeserializationSchemaWrapper";
  private static final String KAFKA_TOPIC_DESCRIPTOR_CLASS =
      "org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor";
  private static final String KAFKA_PARTITION_DISCOVERER_CLASS =
      "org.apache.flink.streaming.connectors.kafka.internals.KafkaPartitionDiscoverer";
  private static final String AVRO_DESERIALIZATION_SCHEMA_CLASS =
      "org.apache.flink.formats.avro.AvroDeserializationSchema";

  private final Object kafkaSource;

  private final ClassLoader userClassLoader;

  @Getter private final Object kafkaSubscriber;

  private KafkaSourceWrapper(
      Object kafkaSource, Object kafkaSubscriber, ClassLoader userClassLoader) {
    this.kafkaSource = kafkaSource;
    this.kafkaSubscriber = kafkaSubscriber;
    this.userClassLoader = userClassLoader;
  }

  public static KafkaSourceWrapper of(Object kafkaSource) throws IllegalAccessException {
    Field subscriberField = FieldUtils.getField(kafkaSource.getClass(), "subscriber", true);
    Object kafkaSubscriber = subscriberField.get(kafkaSource);

    return new KafkaSourceWrapper(
        kafkaSource, kafkaSubscriber, kafkaSource.getClass().getClassLoader());
  }

  public Object getSubscriber() {
    return kafkaSubscriber;
  }

  public Properties getProps() throws IllegalAccessException {
    return WrapperUtils.<Properties>getFieldValue(kafkaSource.getClass(), kafkaSource, "props")
        .get();
  }

  public List<String> getTopics() throws IllegalAccessException {
    Optional<List<String>> topics =
        WrapperUtils.<List<String>>getFieldValue(
            kafkaSubscriber.getClass(), kafkaSubscriber, "topics");

    if (topics.isPresent()) {
      return topics.get();
    }

    Optional<Pattern> topicPattern =
        WrapperUtils.<Pattern>getFieldValue(
            kafkaSubscriber.getClass(), kafkaSubscriber, "topicPattern");

    // TODO: write some unit test to this
    if (topicPattern.isPresent()) {
      try {
        Class descriptorClass = userClassLoader.loadClass(KAFKA_TOPIC_DESCRIPTOR_CLASS);
        Object descriptor =
            descriptorClass
                .getDeclaredConstructor(List.class, Pattern.class)
                .newInstance(null, topicPattern.get());

        Class partitionDiscovererClass =
            userClassLoader.loadClass(KAFKA_PARTITION_DISCOVERER_CLASS);
        Object partitionDiscoverer =
            partitionDiscovererClass
                .getDeclaredConstructor(descriptorClass, int.class, int.class, Properties.class)
                .newInstance(descriptor, 0, 0, getProps());

        WrapperUtils.<List<String>>invoke(
            partitionDiscovererClass, partitionDiscoverer, "initializeConnections");
        return WrapperUtils.<List<String>>invoke(
                partitionDiscovererClass, partitionDiscoverer, "getAllTopics")
            .get();
      } catch (Exception e) {
        log.error("Cannot get all topics from topic pattern ", e);
      }
    }
    return Collections.emptyList();
  }

  public Object getDeserializationSchema() throws IllegalAccessException {
    return WrapperUtils.getFieldValue(kafkaSource.getClass(), kafkaSource, "deserializationSchema")
        .get();
  }

  public Optional<Object> getAvroSchema() {
    try {
      final Class deserializationSchemaWrapperClass =
          userClassLoader.loadClass(DESERIALIZATION_SCHEMA_WRAPPER_CLASS);

      final Class avroDeserializationSchemaClass =
          userClassLoader.loadClass(AVRO_DESERIALIZATION_SCHEMA_CLASS);

      return Optional.of(getDeserializationSchema())
          .filter(el -> el.getClass().isAssignableFrom(deserializationSchemaWrapperClass))
          .flatMap(
              el ->
                  WrapperUtils.<DeserializationSchema>getFieldValue(
                      el.getClass(), el, "deserializationSchema"))
          .filter(schema -> schema.getClass().isAssignableFrom(avroDeserializationSchemaClass))
          .map(
              schema ->
                  WrapperUtils.<TypeInformation>invoke(schema.getClass(), schema, "getProducedType")
                      .get())
          .flatMap(
              typeInformation -> {
                if (typeInformation
                    .getTypeClass()
                    .getCanonicalName()
                    .equals("org.apache.avro.generic.GenericRecord")) {
                  // GenericRecordAvroTypeInfo -> try to extract private schema field
                  return WrapperUtils.<Object>getFieldValue(
                      typeInformation.getClass(), typeInformation, "schema");
                } else {
                  return Optional.ofNullable(typeInformation.getTypeClass())
                      .flatMap(
                          aClass -> WrapperUtils.<Object>invokeStatic(aClass, "getClassSchema"));
                }
              });
    } catch (ClassNotFoundException | IllegalAccessException e) {
      log.error("Cannot extract Avro schema: ", e);
      return Optional.empty();
    }
  }
}
