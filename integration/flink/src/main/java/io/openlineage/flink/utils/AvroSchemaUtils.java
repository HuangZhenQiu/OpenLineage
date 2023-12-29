/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Utility class for translating Avro schema into open lineage schema */
@Slf4j
public class AvroSchemaUtils {

  /**
   * Converts Avro Schema to {@link OpenLineage.SchemaDatasetFacet}
   *
   * @param context Openlineage context
   * @param avroSchema schema
   * @return schema dataset facet
   */
  public static OpenLineage.SchemaDatasetFacet convert(
      OpenLineageContext context, Object avroSchema) {
    OpenLineage.SchemaDatasetFacetBuilder builder =
        context.getOpenLineage().newSchemaDatasetFacetBuilder();
    List<OpenLineage.SchemaDatasetFacetFields> fields = new LinkedList<>();
    try {
      Class schemaClass = context.getUserClassLoader().loadClass("org.apache.avro.Schema");
      Optional<List<Object>> fieldsOpt =
          WrapperUtils.<List<Object>>invoke(schemaClass, avroSchema, "getFields");

      if (fieldsOpt.isPresent()) {
        fieldsOpt.get().stream()
            .forEach(
                avroField -> {
                  try {
                    Class fieldClass =
                        context.getUserClassLoader().loadClass("org.apache.avro.Schema$Field");
                    Optional<String> name =
                        WrapperUtils.<String>invoke(fieldClass, avroField, "name");
                    Optional<Object> filedSchema =
                        WrapperUtils.<Object>invoke(fieldClass, avroField, "schema");
                    Optional<String> doc =
                        WrapperUtils.<String>invoke(fieldClass, avroField, "doc");

                    fields.add(
                        context
                            .getOpenLineage()
                            .newSchemaDatasetFacetFields(
                                name.orElse(""),
                                getTypeName(context.getUserClassLoader(), filedSchema.get())
                                    .orElse(""),
                                doc.orElse("")));
                  } catch (ClassNotFoundException e) {
                    log.debug("Can't find avro schema filed class", e);
                  }
                });
        return builder.fields(fields).build();
      }
    } catch (ClassNotFoundException e) {
      log.debug("Can't find avro schema class", e);
    }
    return builder.fields(Collections.emptyList()).build();
  }

  private static Optional<String> getTypeName(ClassLoader classLoader, Object fieldSchema) {
    // In case of union type containing some type and null type, non-null type name is returned.
    // For example, Avro type `"type": ["null", "long"]` is converted to `long`.

    try {
      Class schemaClass = classLoader.loadClass("org.apache.avro.Schema");
      Class typeClass = classLoader.loadClass("org.apache.avro.Schema$Type");

      boolean isUnion =
          CommonUtils.isInstanceOf(classLoader, fieldSchema, "org.apache.avro.Schema$UnionSchema");

      if (isUnion) {
        Optional<List<Object>> typesOpt =
            WrapperUtils.<List<Object>>invoke(schemaClass, fieldSchema, "getTypes");
        if (typesOpt.isPresent() && typesOpt.get().size() == 2) {
          List<Object> types = typesOpt.get();
          Optional<Object> typeFirst =
              WrapperUtils.<Object>invoke(schemaClass, types.get(0), "getType");
          Optional<Object> typeSecond =
              WrapperUtils.<Object>invoke(schemaClass, types.get(1), "getType");
          Optional<String> firstName =
              WrapperUtils.<String>invoke(typeClass, typeFirst.get(), "getName");
          Optional<String> secondName =
              WrapperUtils.<String>invoke(typeClass, typeSecond.get(), "getName");

          if (firstName.isPresent() && !firstName.get().equals("null")) {
            return firstName;
          } else if (secondName.isPresent() && !secondName.get().equals("null")) {
            return secondName;
          }
        }
      } else {
        Optional<Object> type = WrapperUtils.<Object>invoke(schemaClass, fieldSchema, "getType");
        Optional<String> name = WrapperUtils.<String>invoke(typeClass, type.get(), "getName");
        return name;
      }
    } catch (ClassNotFoundException e) {
      log.debug("Can't find avro schema class", e);
    }

    return Optional.empty();
  }
}
