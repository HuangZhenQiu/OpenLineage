/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.CommonUtils.isInstanceOf;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;

@Slf4j
public class AvroUtils {
  public static final String AVRO_SERIALIZATION_SCHEMA_CLASS =
      "org.apache.flink.formats.avro.AvroSerializationSchema";
  public static final String REGISTRY_AVRO_SERIALIZATION_SCHEMA_CLASS =
      "org.apache.flink.formats.avro.RegistryAvroSerializationSchema";

  public static final String GENERIC_DATUM_WRITER_CLASS =
      "org.apache.avro.generic.GenericDatumWriter";

  public static Optional<Object> getAvroSchema(
      ClassLoader classLoader, Optional<SerializationSchema> serializationSchema) {

    // First try to get the RegistryAvroSerializationSchema
    Optional<Object> registryAvroSchema =
        serializationSchema
            .filter(
                schema ->
                    isInstanceOf(classLoader, schema, REGISTRY_AVRO_SERIALIZATION_SCHEMA_CLASS))
            .flatMap(
                schema -> {
                  WrapperUtils.invoke(schema.getClass(), schema, "checkAvroInitialized");
                  return WrapperUtils.invoke(schema.getClass(), schema, "getDatumWriter");
                })
            .flatMap(
                writer -> WrapperUtils.<Object>getFieldValue(writer.getClass(), writer, "root"));

    // If not present, try to get the AvroSerializationSchema
    return registryAvroSchema.isPresent()
        ? registryAvroSchema
        : serializationSchema
            .filter(schema -> isInstanceOf(classLoader, schema, AVRO_SERIALIZATION_SCHEMA_CLASS))
            .flatMap(
                schema -> {
                  WrapperUtils.invoke(schema.getClass(), schema, "checkAvroInitialized");
                  return WrapperUtils.invoke(schema.getClass(), schema, "getDatumWriter");
                })
            .flatMap(
                writer -> WrapperUtils.<Object>getFieldValue(writer.getClass(), writer, "root"));
  }
}
