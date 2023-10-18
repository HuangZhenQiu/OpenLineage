/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergHandler implements CatalogHandler {

  private final OpenLineageContext context;

  private static final String TYPE = "type";

  private static final String HIVE_CATALOG = "hive_catalog";

  private static final String DATASET_DELIMITTER = ".";

  private static final Pattern datasetPattern = Pattern.compile("(\\w*)\\.(\\w*)\\.(\\w*)");

  public IcebergHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return (tableCatalog instanceof SparkCatalog) || (tableCatalog instanceof SparkSessionCatalog);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String catalogName = tableCatalog.name();

    String prefix = String.format("spark.sql.catalog.%s", catalogName);
    Map<String, String> conf =
        ScalaConversionUtils.<String, String>fromMap(session.conf().getAll());
    log.info(conf.toString());
    Map<String, String> catalogConf =
        conf.entrySet().stream()
            .filter(x -> x.getKey().startsWith(prefix))
            .filter(x -> x.getKey().length() > prefix.length())
            .collect(
                Collectors.toMap(
                    x -> x.getKey().substring(prefix.length() + 1), // handle dot after prefix
                    Map.Entry::getValue));

    log.info(catalogConf.toString());
    if (catalogConf.isEmpty() || !catalogConf.containsKey(TYPE)) {
      throw new UnsupportedCatalogException(catalogName);
    }
    log.info(catalogConf.get(TYPE));

    String warehouse = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);
    DatasetIdentifier di = PathUtils.fromPath(new Path(warehouse, identifier.toString()));

    if (catalogConf.get(TYPE).equals("hive")) {
      di =
          getHiveDatasetIdentifier(
              catalogConf,
              identifier,
              session,
              catalogConf.get(CatalogProperties.URI),
              Optional.ofNullable(catalogConf.get(HIVE_CATALOG)));
    } else if (catalogConf.get(TYPE).equals("hadoop")) {
      di.withSymlink(
          identifier.toString(),
          StringUtils.substringBeforeLast(
              di.getName(), File.separator), // parent location from a name becomes a namespace
          DatasetIdentifier.SymlinkType.TABLE);
    } else if (catalogConf.get(TYPE).equals("rest")) {
      di.withSymlink(
          getRestIdentifier(
              session, catalogConf.get(CatalogProperties.URI), identifier.toString()));
    } else if (catalogConf.get(TYPE).equals("nessie")) {
      di.withSymlink(
          getNessieIdentifier(
              session, catalogConf.get(CatalogProperties.URI), identifier.toString()));
    }
    return di;
  }

  @SneakyThrows
  private DatasetIdentifier getHiveDatasetIdentifier(
      Map<String, String> catalogConf,
      Identifier identifier,
      SparkSession session,
      @Nullable String confUri,
      Optional<String> hiveCatalogName) {
    String icerbergCatalogLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);

    String defaultWarehouseLocation =
        SparkConfUtils.getMetastoreWarehouseURI(session.sparkContext().conf())
            .orElse(new URI("file:/tmp/warehouse"))
            .getPath();

    String warehouseLocation =
        Optional.ofNullable(icerbergCatalogLocation).orElse(defaultWarehouseLocation);

    log.info(
        "For Hive dataset identifier={}, warehouseLocation={}",
        identifier.toString(),
        warehouseLocation);
    DatasetIdentifier di = PathUtils.fromPath(new Path(warehouseLocation, identifier.toString()));

    di.withSymlink(getHiveIdentifier(session, confUri, identifier.toString(), hiveCatalogName));
    return di;
  }

  @SneakyThrows
  private DatasetIdentifier.Symlink getNessieIdentifier(
      SparkSession session, @Nullable String confUri, String table) {

    String uri = new URI(confUri).toString();
    return new DatasetIdentifier.Symlink(table, uri, DatasetIdentifier.SymlinkType.TABLE);
  }

  @SneakyThrows
  private DatasetIdentifier.Symlink getHiveIdentifier(
      SparkSession session,
      @Nullable String confUri,
      String table,
      Optional<String> hiveCatalogName) {

    String catalogName =
        hiveCatalogName.orElse(
            SparkConfUtils.getMetastoreDefaultCatalog(session.sparkContext().conf()).orElse(null));

    boolean catalogNamingFormat = datasetPattern.matcher(table).matches();
    String qualifiedName =
        catalogName != null && !catalogNamingFormat
            ? catalogName + DATASET_DELIMITTER + table
            : table;

    String slashPrefixedTable = String.format("/%s", qualifiedName);
    URI uri;
    if (confUri == null) {
      uri =
          SparkConfUtils.getMetastoreUri(session.sparkContext().conf())
              .orElseThrow(() -> new UnsupportedCatalogException("hive"));
    } else {
      uri = new URI(confUri);
    }
    DatasetIdentifier metastoreIdentifier =
        PathUtils.fromPath(
            new Path(PathUtils.enrichHiveMetastoreURIWithTableName(uri, slashPrefixedTable)));

    return new DatasetIdentifier.Symlink(
        metastoreIdentifier.getName(),
        metastoreIdentifier.getNamespace(),
        DatasetIdentifier.SymlinkType.TABLE);
  }

  @SneakyThrows
  private DatasetIdentifier.Symlink getRestIdentifier(
      SparkSession session, @Nullable String confUri, String table) {

    String uri = new URI(confUri).toString();
    return new DatasetIdentifier.Symlink(table, uri, DatasetIdentifier.SymlinkType.TABLE);
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    String format = properties.getOrDefault("format", "");
    return Optional.of(
        context.getOpenLineage().newStorageDatasetFacet("iceberg", format.replace("iceberg/", "")));
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    SparkTable table;
    try {
      table = (SparkTable) tableCatalog.loadTable(identifier);
    } catch (NoSuchTableException | ClassCastException e) {
      log.error("Failed to load table from catalog: {}", identifier, e);
      return Optional.empty();
    }

    if (table.table() != null && table.table().currentSnapshot() != null) {
      return Optional.of(Long.toString(table.table().currentSnapshot().snapshotId()));
    }
    return Optional.empty();
  }

  @Override
  public String getName() {
    return "iceberg";
  }
}
