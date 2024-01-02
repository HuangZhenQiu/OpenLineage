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
    Optional<Object> schemaOpt = WrapperUtils.invoke(table.getClass(), table, "schema");
    if (schemaOpt.isPresent()) {
      Optional<List<Object>> schemaFieldsOpt =
          WrapperUtils.<List<Object>>invoke(schemaOpt.get().getClass(), schemaOpt.get(), "columns");

      if (schemaFieldsOpt.isPresent()) {
        List<SchemaDatasetFacetFields> fields =
            schemaFieldsOpt.get().stream()
                .map(
                    field -> {
                      Optional<String> name =
                          WrapperUtils.<String>invoke(field.getClass(), field, "name");
                      Optional<String> doc =
                          WrapperUtils.<String>invoke(field.getClass(), field, "doc");
                      Optional<Object> type =
                          WrapperUtils.<Object>invoke(field.getClass(), field, "type");
                      Optional<Object> typeId =
                          type.isPresent()
                              ? WrapperUtils.<Object>invoke(
                                  type.get().getClass(), type.get(), "typeId")
                              : Optional.empty();
                      return context
                          .getOpenLineage()
                          .newSchemaDatasetFacetFields(
                              name.orElse(""), typeId.orElse("").toString(), doc.orElse(""));
                    })
                .collect(Collectors.toList());
        return context.getOpenLineage().newSchemaDatasetFacet(fields);
      } else {
        log.info("Columns in iceberg schema is not found");
      }
    } else {
      log.info("Columns in iceberg schema is not found");
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
