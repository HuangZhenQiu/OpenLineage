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
    tableLoaderOpt.map(
        tableLoader -> {
          if (tableLoader instanceof TableLoader.CatalogTableLoader) {
            return getCatalogName((TableLoader.CatalogTableLoader) tableLoader);
          } else if (tableLoader instanceof TableLoader.HadoopTableLoader) {
            return Optional.of(Constants.HADOOP_CATALOG_NAME);
          } else {
            log.warn(
                "Failed to find the catalog namespace due to unsupported table loader in Iceberg Sink.");
            return Optional.empty();
          }
        });
    return Optional.empty();
  }

  private static Optional<String> getCatalogName(
      TableLoader.CatalogTableLoader catalogTableLoader) {
    Class catalogLoaderClass = null;
    try {
      catalogLoaderClass = Class.forName("org.apache.iceberg.flink.CatalogTableLoader");
      Optional<CatalogLoader> catalogLoaderOpt =
          WrapperUtils.<CatalogLoader>getFieldValue(
              catalogLoaderClass, catalogTableLoader, "tableLoader");
      return catalogLoaderOpt.map(catalogLoader -> catalogLoader.loadCatalog().name());
    } catch (ClassNotFoundException e) {
      log.warn("Failed extracting catalog Loader from CatalogTableLoader", e);
    }

    return Optional.empty();
  }
}
