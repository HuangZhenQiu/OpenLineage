/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifierUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.CommonUtils;
import io.openlineage.flink.utils.Constants;
import io.openlineage.flink.utils.IcebergUtils;
import io.openlineage.flink.visitor.wrapper.IcebergSourceWrapper;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public static final String STREAMING_MONITOR_FUNCTION =
      "org.apache.iceberg.flink.source.StreamingMonitorFunction";

  public static final String ICEBERG_SOURCE = "org.apache.iceberg.flink.source.IcebergSource";

  public static final String ICEBERG_TABLE_SOURCE =
      "org.apache.iceberg.flink.source.IcebergTableSource";

  public IcebergSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    return isInstanceOf(source, STREAMING_MONITOR_FUNCTION)
        || isInstanceOf(source, ICEBERG_SOURCE)
        || isInstanceOf(source, ICEBERG_TABLE_SOURCE);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object source) {
    IcebergSourceWrapper sourceWrapper;
    try {
      if (isInstanceOf(source, STREAMING_MONITOR_FUNCTION)) {
        Class streamingMonitorFunction =
            context.getUserClassLoader().loadClass(STREAMING_MONITOR_FUNCTION);
        sourceWrapper =
            IcebergSourceWrapper.of(source, streamingMonitorFunction, context.getUserClassLoader());
      } else if (isInstanceOf(source, ICEBERG_SOURCE)) {
        Class icebergSource = context.getUserClassLoader().loadClass(ICEBERG_SOURCE);
        sourceWrapper =
            IcebergSourceWrapper.of(source, icebergSource, context.getUserClassLoader());
      } else if (isInstanceOf(source, ICEBERG_TABLE_SOURCE)) {
        Class icebergTableSource = context.getUserClassLoader().loadClass(ICEBERG_TABLE_SOURCE);
        sourceWrapper =
            IcebergSourceWrapper.of(source, icebergTableSource, context.getUserClassLoader());
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported Iceberg Source type %s", source.getClass().getCanonicalName()));
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return Collections.singletonList(
        getDataset(context, sourceWrapper.getTable(), sourceWrapper.getNamespace()));
  }

  private OpenLineage.InputDataset getDataset(
      OpenLineageContext context, Optional<Object> table, Optional<String> namespaceOpt) {
    OpenLineage openLineage = context.getOpenLineage();

    try {
      if (table.isPresent()) {
        Class tableClass = context.getUserClassLoader().loadClass("org.apache.iceberg.Table");
        Optional<String> location = WrapperUtils.invoke(tableClass, table.get(), "location");
        Optional<String> name = WrapperUtils.invoke(tableClass, table.get(), "name");

        DatasetIdentifier datasetIdentifier =
            DatasetIdentifierUtils.fromURI(URI.create(location.orElse("")));

        OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
            CommonUtils.createSymlinkFacet(
                context.getOpenLineage(),
                Constants.TABLE_TYPE,
                name.orElse(""),
                namespaceOpt.orElse(""));
        return openLineage
            .newInputDatasetBuilder()
            .name(datasetIdentifier.getName())
            .namespace(datasetIdentifier.getNamespace())
            .facets(
                openLineage
                    .newDatasetFacetsBuilder()
                    .schema(IcebergUtils.getSchema(context, table.get()))
                    .symlinks(symlinksDatasetFacet)
                    .build())
            .build();
      }
    } catch (ClassNotFoundException e) {
      log.error("Class iceberg table is not found", e);
    }

    return openLineage.newInputDatasetBuilder().build();
  }
}
