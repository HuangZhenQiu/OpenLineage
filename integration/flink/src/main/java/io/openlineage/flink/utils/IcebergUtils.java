/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

@Slf4j
public class IcebergUtils {

  public static boolean hasClasses() {
    try {
      IcebergUtils.class
          .getClassLoader()
          .loadClass("org.apache.iceberg.flink.source.StreamingMonitorFunction");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static OpenLineage.SchemaDatasetFacet getSchema(OpenLineageContext context, Table table) {
    List<SchemaDatasetFacetFields> fields =
        table.schema().columns().stream()
            .map(
                field ->
                    context
                        .getOpenLineage()
                        .newSchemaDatasetFacetFields(
                            field.name(), field.type().typeId().name(), field.doc()))
            .collect(Collectors.toList());
    return context.getOpenLineage().newSchemaDatasetFacet(fields);
  }

  public static Optional<String> getNamespace(Optional<TableLoader> tableLoaderOpt) {
    return tableLoaderOpt
        .map(
            tableLoader -> {
              if (tableLoader instanceof TableLoader.HadoopTableLoader) {
                TableLoader.HadoopTableLoader hadoopTableLoader =
                    (TableLoader.HadoopTableLoader) tableLoader;
                return WrapperUtils.<String>getFieldValue(
                    TableLoader.HadoopTableLoader.class, hadoopTableLoader, "location");
              } else if (tableLoader instanceof TableLoader.CatalogTableLoader) {
                TableLoader.CatalogTableLoader catalogTableLoader =
                    (TableLoader.CatalogTableLoader) tableLoader;
                return getNamespace(catalogTableLoader);
              } else {
                throw new UnsupportedOperationException(
                    String.format(
                        "Unsupported operation to get namespace from table loader type %s",
                        tableLoader));
              }
            })
        .orElse(Optional.empty());
  }

  private static Optional<String> getNamespace(TableLoader.CatalogTableLoader catalogTableLoader) {
    Optional<CatalogLoader> catalogLoaderOpt =
        WrapperUtils.<CatalogLoader>getFieldValue(
            TableLoader.CatalogTableLoader.class, catalogTableLoader, "catalogLoader");
    return catalogLoaderOpt
        .map(
            catalogLoader -> {
              if (catalogLoader instanceof CatalogLoader.HiveCatalogLoader) {
                CatalogLoader.HiveCatalogLoader hiveCatalogLoader =
                    (CatalogLoader.HiveCatalogLoader) catalogLoader;
                return (WrapperUtils.<String>getFieldValue(
                    CatalogLoader.HiveCatalogLoader.class, hiveCatalogLoader, "uri"));
              } else if (catalogLoader instanceof CatalogLoader.HadoopCatalogLoader) {
                CatalogLoader.HadoopCatalogLoader hadoopCatalogLoader =
                    (CatalogLoader.HadoopCatalogLoader) catalogLoader;
                return WrapperUtils.<String>getFieldValue(
                    CatalogLoader.HadoopCatalogLoader.class,
                    hadoopCatalogLoader,
                    "warehouseLocation");
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
