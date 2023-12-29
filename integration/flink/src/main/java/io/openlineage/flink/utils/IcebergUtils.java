/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static io.openlineage.flink.utils.CommonUtils.isInstanceOf;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergUtils {

  public static boolean hasClasses(ClassLoader classLoader) {
    try {
      classLoader.loadClass("org.apache.iceberg.flink.source.StreamingMonitorFunction");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static OpenLineage.SchemaDatasetFacet getSchema(OpenLineageContext context, Object table) {
    try {
      Class tableClass = context.getUserClassLoader().loadClass("org.apache.iceberg.Table");
      Class schemaClass = context.getUserClassLoader().loadClass("org.apache.iceberg.Schema");
      Class nestedFieldClass = Class.forName("org.apache.iceberg.types.Types$NestedField");
      Class typeClass = context.getUserClassLoader().loadClass("org.apache.iceberg.types.Type");

      Optional<Object> schemaOpt = WrapperUtils.invoke(tableClass, table, "schema");
      if (schemaOpt.isPresent()) {
        Optional<List<Object>> schemaFieldsOpt =
            WrapperUtils.<List<Object>>invoke(schemaClass, schemaOpt.get(), "columns");

        if (schemaFieldsOpt.isPresent()) {
          List<SchemaDatasetFacetFields> fields =
              schemaFieldsOpt.get().stream()
                  .map(
                      field -> {
                        Optional<String> name =
                            WrapperUtils.<String>invoke(nestedFieldClass, field, "name");
                        Optional<String> doc =
                            WrapperUtils.<String>invoke(nestedFieldClass, field, "doc");
                        Optional<Object> type =
                            WrapperUtils.<Object>invoke(nestedFieldClass, field, "type");
                        Optional<Object> typeId =
                            WrapperUtils.<Object>invoke(typeClass, type.get(), "typeId");
                        return context
                            .getOpenLineage()
                            .newSchemaDatasetFacetFields(
                                name.orElse(""), typeId.orElse("").toString(), doc.orElse(""));
                      })
                  .collect(Collectors.toList());
          return context.getOpenLineage().newSchemaDatasetFacet(fields);
        }
      }
    } catch (ClassNotFoundException e) {
      log.debug("Required iceberg class for schema inference is not found", e);
    }
    return context.getOpenLineage().newSchemaDatasetFacet(List.of());
  }

  public static Optional<String> getNamespace(
      ClassLoader classLoader, Optional<Object> tableLoaderOpt) {
    return tableLoaderOpt
        .map(
            tableLoader -> {
              if (isInstanceOf(
                  classLoader,
                  tableLoader,
                  "org.apache.iceberg.flink.TableLoader$HadoopTableLoader")) {
                return WrapperUtils.<String>getFieldValue(
                    tableLoader.getClass(), tableLoader, "location");
              } else if (isInstanceOf(
                  classLoader,
                  tableLoader,
                  "org.apache.iceberg.flink.TableLoader$CatalogTableLoader")) {
                return getNamespace(classLoader, tableLoader);
              } else {
                throw new UnsupportedOperationException(
                    String.format(
                        "Unsupported operation to get namespace from table loader type %s",
                        tableLoader));
              }
            })
        .orElse(Optional.empty());
  }

  private static Optional<String> getNamespace(ClassLoader classLoader, Object catalogTableLoader) {
    Optional<Object> catalogLoaderOpt =
        WrapperUtils.getFieldValue(
            catalogTableLoader.getClass(), catalogTableLoader, "catalogLoader");

    return catalogLoaderOpt
        .map(
            catalogLoader -> {
              if (isInstanceOf(
                  classLoader,
                  catalogLoader,
                  "org.apache.iceberg.flink.CatalogLoader$HiveCatalogLoader")) {
                return (WrapperUtils.<String>getFieldValue(
                    catalogLoader.getClass(), catalogLoader, "uri"));
              } else if (isInstanceOf(
                  classLoader,
                  catalogLoader,
                  "org.apache.iceberg.flink.CatalogLoader$HadoopCatalogLoader")) {
                return WrapperUtils.<String>getFieldValue(
                    catalogLoader.getClass(), catalogLoader, "warehouseLocation");
              } else {
                throw new UnsupportedOperationException(
                    String.format(
                        "Unsupported operation to get namespace from catalog loader type %s",
                        catalogTableLoader));
              }
            })
        .orElse(Optional.empty());
  }
}
