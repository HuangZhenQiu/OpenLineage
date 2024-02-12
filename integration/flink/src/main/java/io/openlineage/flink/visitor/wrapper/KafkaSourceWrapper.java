/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.Constants.DESERIALIZATION_SCHEMA_WRAPPER_CLASS;
import static io.openlineage.flink.utils.Constants.DYNAMIC_DESERIALIZATION_SCHEMA_CLASS;
import static io.openlineage.flink.utils.Constants.KAFKA_TOPIC_DESCRIPTOR_CLASS;
import static io.openlineage.flink.utils.Constants.VALUE_ONLY_DESERIALIZATION_SCHEMA_WRAPPER_CLASS;

import io.openlineage.flink.utils.KafkaUtils;
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

        return KafkaUtils.getAllTopics(userClassLoader, descriptor, getProps());
      } catch (Exception e) {
        log.debug("Cannot get all topics from topic pattern ", e);
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
      final Class valueOnlyDeserializationSchemaWrapperClass =
          userClassLoader.loadClass(VALUE_ONLY_DESERIALIZATION_SCHEMA_WRAPPER_CLASS);

      final Class deserializationSchemaWrapperClass =
          userClassLoader.loadClass(DESERIALIZATION_SCHEMA_WRAPPER_CLASS);

      final Class dynamicDeserializationSchemaWrapperClass =
          userClassLoader.loadClass(DYNAMIC_DESERIALIZATION_SCHEMA_CLASS);

      Object recordDeserializationSchema = getDeserializationSchema();
      if (valueOnlyDeserializationSchemaWrapperClass.isAssignableFrom(
          recordDeserializationSchema.getClass())) {
        return convert(
            WrapperUtils.<DeserializationSchema>getFieldValue(
                valueOnlyDeserializationSchemaWrapperClass,
                recordDeserializationSchema,
                "deserializationSchema"));
      } else if (deserializationSchemaWrapperClass.isAssignableFrom(
          recordDeserializationSchema.getClass())) {
        Optional<Object> deserializationSchemaOpt =
            WrapperUtils.<Object>getFieldValue(
                deserializationSchemaWrapperClass,
                recordDeserializationSchema,
                "kafkaDeserializationSchema");

        if (deserializationSchemaOpt.isPresent()) {
          return convert(
              WrapperUtils.<DeserializationSchema>getFieldValue(
                  dynamicDeserializationSchemaWrapperClass,
                  deserializationSchemaOpt.get(),
                  "valueDeserialization"));
        }
      }

      return Optional.empty();
    } catch (ClassNotFoundException | IllegalAccessException e) {
      log.debug("Cannot extract Avro schema: ", e);
      return Optional.empty();
    }
  }

  private Optional<Object> convert(Optional<DeserializationSchema> schemaOpt) {
    if (schemaOpt.isPresent() && schemaOpt.get().getProducedType() != null) {
      return convert(schemaOpt.get().getProducedType());
    }

    return Optional.empty();
  }

  private Optional<Object> convert(TypeInformation<?> typeInformation) {
    if (typeInformation.getTypeClass().equals(org.apache.avro.generic.GenericRecord.class)) {
      // GenericRecordAvroTypeInfo -> try to extract private schema field
      return WrapperUtils.<Object>getFieldValue(
          typeInformation.getClass(), typeInformation, "schema");
    } else {
      return Optional.ofNullable(typeInformation.getTypeClass())
          .flatMap(aClass -> WrapperUtils.<Object>invokeStatic(aClass, "getClassSchema"));
    }
  }
}
