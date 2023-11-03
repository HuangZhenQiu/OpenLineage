/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;

public class IcebergUtilsTest {

  @Test
  void testGetNamespaceForHadoopTableLoader() {
    Configuration conf = new Configuration();
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://test", conf);
    assertEquals("hdfs://test", IcebergUtils.getNamespace(Optional.of(tableLoader)).get());
  }

  @Test
  void testGetNamespaceForCatalogTableLoaderInHiveCatalog() {
    Configuration conf = new Configuration();
    Map<String, String> properties = Map.of("uri", "thrift://localhost:9083");
    CatalogLoader catalogLoader = CatalogLoader.hive("hive-catalog", conf, properties);
    TableIdentifier tableIdentifier = TableIdentifier.parse("hive.local.table");
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
    assertEquals(
        "thrift://localhost:9083", IcebergUtils.getNamespace(Optional.of(tableLoader)).get());
  }

  @Test
  void testGetNamespaceForCatalogTableLoaderInHadoopCatalog() {
    Configuration conf = new Configuration();
    Map<String, String> properties = Map.of("warehouse", "hdfs://test");
    CatalogLoader catalogLoader = CatalogLoader.hadoop("hive-catalog", conf, properties);
    TableIdentifier tableIdentifier = TableIdentifier.parse("hive.local.table");
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
    assertEquals("hdfs://test", IcebergUtils.getNamespace(Optional.of(tableLoader)).get());
  }
}
