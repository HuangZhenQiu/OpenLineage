/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.CommonUtils;
import io.openlineage.flink.utils.Constants;
import io.openlineage.flink.visitor.wrapper.CassandraSourceWrapper;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public static final String CASSANDRA_INPUT_FORMAT_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraInputFormat";
  public static final String CASSANDRA_POJO_INPUT_FORMAT_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat";

  public CassandraSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return isInstanceOf(object, CASSANDRA_INPUT_FORMAT_CLASS)
        || isInstanceOf(object, CASSANDRA_POJO_INPUT_FORMAT_CLASS);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    log.debug("Apply source {} in CassandraSourceVisitor", object);
    CassandraSourceWrapper sourceWrapper;
    if (isInstanceOf(object, CASSANDRA_INPUT_FORMAT_CLASS)
        || isInstanceOf(object, CASSANDRA_POJO_INPUT_FORMAT_CLASS)) {
      sourceWrapper =
          CassandraSourceWrapper.of(context.getUserClassLoader(), object, object.getClass(), true);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Cassandra Source type %s", object.getClass().getCanonicalName()));
    }

    return Collections.singletonList(
        getDataset(
            context,
            (String) sourceWrapper.getNamespace().get(),
            (String) sourceWrapper.getTableName().get()));
  }

  private OpenLineage.InputDataset getDataset(
      OpenLineageContext context, String namespace, String name) {
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
        CommonUtils.createSymlinkFacet(
            context.getOpenLineage(), Constants.CASSANDRA_TYPE, name, namespace);
    OpenLineage.DatasetFacets datasetFacets =
        outputDataset().getDatasetFacetsBuilder().symlinks(symlinksDatasetFacet).build();

    return openLineage
        .newInputDatasetBuilder()
        .name(name)
        .namespace(namespace)
        .facets(datasetFacets)
        .build();
  }
}
