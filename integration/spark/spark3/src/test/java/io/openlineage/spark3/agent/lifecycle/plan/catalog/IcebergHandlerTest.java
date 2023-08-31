/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.collection.immutable.Map;

class IcebergHandlerTest {

  private OpenLineageContext context = mock(OpenLineageContext.class);
  private IcebergHandler icebergHandler = new IcebergHandler(context);
  private SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  private RuntimeConfig runtimeConfig = mock(RuntimeConfig.class);

  @ParameterizedTest
  @CsvSource({
    "hdfs://namenode:8020/tmp/warehouse,hdfs://namenode:8020,/tmp/warehouse/database.schema.table",
    "/tmp/warehouse,file,/tmp/warehouse/database.schema.table"
  })
  void testGetDatasetIdentifierForHadoop(String warehouseConf, String namespace, String name) {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map2<>(
                "spark.sql.catalog.test.type",
                "hadoop",
                "spark.sql.catalog.test.warehouse",
                warehouseConf));

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    assertEquals(name, datasetIdentifier.getName());
    assertEquals(namespace, datasetIdentifier.getNamespace());
    assertEquals("database.schema.table", datasetIdentifier.getSymlinks().get(0).getName());
    assertEquals(
        StringUtils.substringBeforeLast(name, "/"),
        datasetIdentifier.getSymlinks().get(0).getNamespace());
  }

  @Test
  void testGetDatasetIdentifierForHive() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.sql.hive.metastore.uris", "thrift://metastore-host:10001");
    sparkConf.set("spark.hadoop.metastore.catalog.default", "default_catalog");

    Map.Map4<String, String> config =
        new Map.Map4<>(
            "spark.sql.catalog.test.type",
            "hive",
            "spark.sql.catalog.test.hive_catalog",
            "catalog_name",
            "spark.sql.catalog.test.uri",
            "thrift://metastore-host:10001",
            "spark.sql.catalog.test.warehouse",
            "/tmp/warehouse");

    java.util.Map configMap = ScalaConversionUtils.fromMap(config);
    configMap.forEach((k, v) -> sparkConf.set(k.toString(), v.toString()));
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll()).thenReturn(config);
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    DatasetIdentifier.Symlink symlink = datasetIdentifier.getSymlinks().get(0);
    assertEquals("/tmp/warehouse/database.schema.table", datasetIdentifier.getName());
    assertEquals("file", datasetIdentifier.getNamespace());
    assertEquals("database.schema.table", symlink.getName());
    assertEquals("hive://metastore-host:10001", symlink.getNamespace());
    assertEquals("TABLE", symlink.getType().toString());

    datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"schema"}, "table"),
            new HashMap<>());

    symlink = datasetIdentifier.getSymlinks().get(0);
    assertEquals("/tmp/warehouse/schema.table", datasetIdentifier.getName());
    assertEquals("file", datasetIdentifier.getNamespace());
    assertEquals("catalog_name.schema.table", symlink.getName());
    assertEquals("hive://metastore-host:10001", symlink.getNamespace());
    assertEquals("TABLE", symlink.getType().toString());

    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.test.type",
                "hive",
                "spark.hadoop.metastore.catalog.default",
                "default_catalog",
                "spark.sql.catalog.test.uri",
                "thrift://metastore-host:10001"));

    datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"schema"}, "table"),
            new HashMap<>());

    symlink = datasetIdentifier.getSymlinks().get(0);
    assertEquals("/tmp/warehouse/schema.table", datasetIdentifier.getName());
    assertEquals("file", datasetIdentifier.getNamespace());
    assertEquals("default_catalog.schema.table", symlink.getName());
    assertEquals("hive://metastore-host:10001", symlink.getNamespace());
    assertEquals("TABLE", symlink.getType().toString());

    HashMap<String, String> configMapWithHadoopWarehouseDir = new HashMap<>(configMap);
    configMapWithHadoopWarehouseDir.put(
        "spark.hadoop.hive.metastore.warehouse.dir", "/user/hive/warehouse/");
    configMapWithHadoopWarehouseDir.forEach((k, v) -> sparkConf.set(k, v));

    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<String, String>(
                "spark.sql.catalog.test.type",
                "hive",
                "spark.sql.catalog.test.hive_catalog",
                "catalog_name",
                "spark.sql.catalog.test.uri",
                "thrift://metastore-host:10001"));

    datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"schema"}, "table"),
            new HashMap<>());

    symlink = datasetIdentifier.getSymlinks().get(0);
    assertEquals("/user/hive/warehouse/schema.table", datasetIdentifier.getName());
    assertEquals("file", datasetIdentifier.getNamespace());
    assertEquals("catalog_name.schema.table", symlink.getName());
    assertEquals("hive://metastore-host:10001", symlink.getNamespace());
    assertEquals("TABLE", symlink.getType().toString());
  }

  @Test
  void testGetDatasetIdentifierForRest() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getAll())
        .thenReturn(
            new Map.Map3<>(
                "spark.sql.catalog.iceberg.type",
                "rest",
                "spark.sql.catalog.iceberg.uri",
                "http://lakehouse-host:8080",
                "spark.sql.catalog.iceberg.warehouse",
                "s3a://lakehouse/"));
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("iceberg");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            sparkSession,
            sparkCatalog,
            Identifier.of(new String[] {"schema"}, "table"),
            new HashMap<>());

    DatasetIdentifier.Symlink symlink = datasetIdentifier.getSymlinks().get(0);
    assertEquals("schema.table", datasetIdentifier.getName());
    assertEquals("s3a://lakehouse", datasetIdentifier.getNamespace());
    // symlink
    assertEquals("schema.table", symlink.getName());
    assertEquals("http://lakehouse-host:8080", symlink.getNamespace());
    assertEquals("TABLE", symlink.getType().toString());
  }

  @Test
  void testGetStorageDatasetFacet() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    Optional<OpenLineage.StorageDatasetFacet> storageDatasetFacet =
        icebergHandler.getStorageDatasetFacet(
            Collections.singletonMap("format", "iceberg/parquet"));
    assertEquals("iceberg", storageDatasetFacet.get().getStorageLayer());
    assertEquals("parquet", storageDatasetFacet.get().getFileFormat());
  }

  @Test
  void testStorageDatasetFacetWhenFormatNotProvided() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    Optional<OpenLineage.StorageDatasetFacet> storageDatasetFacet =
        icebergHandler.getStorageDatasetFacet(new HashMap<>());
    assertEquals("iceberg", storageDatasetFacet.get().getStorageLayer());
    assertEquals("", storageDatasetFacet.get().getFileFormat());
  }

  @Test
  void testGetVersionString() throws NoSuchTableException {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");

    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().currentSnapshot().snapshotId()).thenReturn(1500100900L);

    Optional<String> version =
        icebergHandler.getDatasetVersion(sparkCatalog, identifier, Collections.emptyMap());

    assertTrue(version.isPresent());
    assertEquals(version.get(), "1500100900");
  }
}
