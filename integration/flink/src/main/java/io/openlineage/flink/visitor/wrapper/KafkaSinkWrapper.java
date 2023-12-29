/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Wrapper class to extract hidden fields and call hidden methods on KafkaSink object. It
 * encapsulates all the reflection methods used on KafkaSink.
 */
@Slf4j
public class KafkaSinkWrapper {
  private final Object kafkaSink;
  private final Object serializationSchema;
  private final ClassLoader userClassLoader;

  private KafkaSinkWrapper(Object kafkaSink, ClassLoader userClassLoader) {
    this.kafkaSink = kafkaSink;
    this.serializationSchema =
        WrapperUtils.<Object>getFieldValue(kafkaSink.getClass(), kafkaSink, "recordSerializer")
            .get();
    this.userClassLoader = userClassLoader;
  }

  public static KafkaSinkWrapper of(Object kafkaSink, ClassLoader userClassLoader) {
    return new KafkaSinkWrapper(kafkaSink, userClassLoader);
  }

  public Properties getKafkaProducerConfig() {
    return WrapperUtils.<Properties>getFieldValue(
            kafkaSink.getClass(), kafkaSink, "kafkaProducerConfig")
        .get();
  }

  public String getKafkaTopic() throws IllegalAccessException {
    Function<?, ?> topicSelector =
        WrapperUtils.<Function<?, ?>>getFieldValue(
                serializationSchema.getClass(), serializationSchema, "topicSelector")
            .get();

    Function<?, ?> function =
        (Function<?, ?>)
            WrapperUtils.getFieldValue(topicSelector.getClass(), topicSelector, "topicSelector")
                .get();

    return (String) function.apply(null);
  }

  public Optional<Object> getAvroSchema() {
    return AvroUtils.getAvroSchema(
        userClassLoader,
        WrapperUtils.<SerializationSchema>getFieldValue(
            serializationSchema.getClass(), serializationSchema, "valueSerializationSchema"));
  }
}
