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
import io.openlineage.flink.visitor.wrapper.IcebergSinkWrapper;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

@Slf4j
public class IcebergSinkVisitor extends Visitor<OpenLineage.OutputDataset> {

  public IcebergSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    return sink instanceof OneInputTransformation
        && ((OneInputTransformation) sink)
            .getOperatorFactory()
            .getStreamOperatorClass(context.getUserClassLoader())
            .getCanonicalName()
            .equals("org.apache.iceberg.flink.sink.IcebergFilesCommitter");
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object icebergSink) {
    IcebergSinkWrapper sinkWrapper =
        IcebergSinkWrapper.of(
            ((OneInputTransformation) icebergSink).getOperator(), context.getUserClassLoader());
    return sinkWrapper
        .getTable()
        .map(table -> getDataset(context, table, sinkWrapper.getNamespace()))
        .map(dataset -> Collections.singletonList(dataset))
        .orElse(Collections.emptyList());
  }

  private OpenLineage.OutputDataset getDataset(
      OpenLineageContext context, Object table, Optional<String> namespaceOpt) {
    OpenLineage openLineage = context.getOpenLineage();
    try {
      Class tableClass = context.getUserClassLoader().loadClass("org.apache.iceberg.Table");
      Optional<String> location = WrapperUtils.invoke(tableClass, table, "location");
      Optional<String> name = WrapperUtils.invoke(tableClass, table, "name");

      DatasetIdentifier datasetIdentifier =
          DatasetIdentifierUtils.fromURI(URI.create(location.orElse("")));
      OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
          CommonUtils.createSymlinkFacet(
              context.getOpenLineage(),
              Constants.TABLE_TYPE,
              name.orElse(""),
              namespaceOpt.orElse(""));

      return openLineage
          .newOutputDatasetBuilder()
          .name(datasetIdentifier.getName())
          .namespace(datasetIdentifier.getNamespace())
          .facets(
              openLineage
                  .newDatasetFacetsBuilder()
                  .schema(IcebergUtils.getSchema(context, table))
                  .symlinks(symlinksDatasetFacet)
                  .build())
          .build();
    } catch (ClassNotFoundException e) {
      log.debug("Class iceberg table is not found", e);
    }

    return openLineage.newOutputDatasetBuilder().build();
  }
}
