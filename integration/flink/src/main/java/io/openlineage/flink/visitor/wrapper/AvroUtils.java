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
  public static final String AVRO_ROW_DATA_SERIALIZATION_SCHEMA_CLASS =
      "org.apache.flink.formats.avro.AvroRowDataSerializationSchema";

  public static final String GENERIC_DATUM_WRITER_CLASS =
      "org.apache.avro.generic.GenericDatumWriter";

  public static Optional<Object> getAvroSchema(
      ClassLoader classLoader, Optional<SerializationSchema> serializationSchema) {

    // First try to get the RegistryAvroSerializationSchema
    try {
      Class schemaClass = classLoader.loadClass(AVRO_SERIALIZATION_SCHEMA_CLASS);
      if (serializationSchema.isPresent()) {
        if (isInstanceOf(
            classLoader, serializationSchema.get(), REGISTRY_AVRO_SERIALIZATION_SCHEMA_CLASS)) {
          // Needs to use parent class here to access method defined in parent class
          return serializationSchema
              .flatMap(
                  schema -> {
                    // Needs to use parent class here to access method defined in parent class
                    WrapperUtils.invoke(schemaClass, schema, "checkAvroInitialized");
                    return WrapperUtils.invoke(schemaClass, schema, "getDatumWriter");
                  })
              .flatMap(
                  writer -> WrapperUtils.<Object>getFieldValue(writer.getClass(), writer, "root"));
        } else if (isInstanceOf(
            classLoader, serializationSchema.get(), AVRO_SERIALIZATION_SCHEMA_CLASS)) {
          return serializationSchema
              .flatMap(
                  schema -> {
                    WrapperUtils.invoke(schema.getClass(), schema, "checkAvroInitialized");
                    return WrapperUtils.invoke(schema.getClass(), schema, "getDatumWriter");
                  })
              .flatMap(
                  writer -> WrapperUtils.<Object>getFieldValue(writer.getClass(), writer, "root"));
        } else if (isInstanceOf(
            classLoader, serializationSchema.get(), AVRO_ROW_DATA_SERIALIZATION_SCHEMA_CLASS)) {
          return serializationSchema.flatMap(
              schema -> WrapperUtils.getFieldValue(schema.getClass(), schema, "schema"));
        }
      }
    } catch (ClassNotFoundException e) {
      log.debug("Class RegistryAvroSerializationSchema not found in user jar", e);
    }
    return Optional.empty();
  }
}
