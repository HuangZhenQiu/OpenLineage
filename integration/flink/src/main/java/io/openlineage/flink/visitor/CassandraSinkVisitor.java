/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper.INSERT_QUERY_FIELD_NAME;
import static io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper.POJO_CLASS_FIELD_NAME;
import static io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper.POJO_OUTPUT_CLASS_FIELD_NAME;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.CommonUtils;
import io.openlineage.flink.utils.Constants;
import io.openlineage.flink.visitor.wrapper.CassandraSinkWrapper;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;

@Slf4j
public class CassandraSinkVisitor extends Visitor<OpenLineage.OutputDataset> {
  public static final String CASSANDRA_POJO_OUTPUT_FORMAT_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat";

  public static final String CASSANDRA_ROW_OUTPUT_FORMAT_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraRowOutputFormat";

  public static final String CASSANDRA_TUPLE_OUTPUT_FORMAT_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat";

  public static final String CASSANDRA_POJO_SINK_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink";

  public static final String CASSANDRA_ROW_SINK_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraRowSink";

  public static final String CASSANDRA_TUPLE_SINK_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraTupleSink";

  public static final String CASSANDRA_ROW_WRITE_AHEAD_SINK_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraRowWriteAheadSink";

  public static final String CASSANDRA_SCALA_PRODUCT_SINK_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraScalaProductSink";

  public static final String CASSANDRA_TUPLE_WRITE_AHEAD_SINK_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraTupleWriteAheadSink";

  public CassandraSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return isInstanceOf(object, CASSANDRA_POJO_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, CASSANDRA_ROW_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, CASSANDRA_TUPLE_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, CASSANDRA_POJO_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_ROW_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_TUPLE_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_ROW_WRITE_AHEAD_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_SCALA_PRODUCT_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_TUPLE_WRITE_AHEAD_SINK_CLASS);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object object) {
    log.debug("Apply sink {} in CassandraSinkVisitor", object);
    CassandraSinkWrapper sinkWrapper = null;
    if (object instanceof RichOutputFormat) {
      sinkWrapper = createWrapperForOutputFormat(object);
    } else {
      sinkWrapper = createWrapperForSink(object);
    }

    if (sinkWrapper == null) {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Cassandra sink type %s", object.getClass().getCanonicalName()));
    }

    return Collections.singletonList(
        getDataset(
            context,
            (String) sinkWrapper.getNamespace().get(),
            (String) sinkWrapper.getTableName().get()));
  }

  private CassandraSinkWrapper createWrapperForSink(Object object) {
    CassandraSinkWrapper sinkWrapper = null;
    if (isInstanceOf(object, CASSANDRA_POJO_SINK_CLASS)) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              context.getUserClassLoader(),
              object,
              object.getClass(),
              POJO_CLASS_FIELD_NAME,
              false);
    } else if (isInstanceOf(object, CASSANDRA_TUPLE_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_ROW_WRITE_AHEAD_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_SCALA_PRODUCT_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_TUPLE_WRITE_AHEAD_SINK_CLASS)
        || isInstanceOf(object, CASSANDRA_ROW_SINK_CLASS)) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              context.getUserClassLoader(),
              object,
              object.getClass(),
              INSERT_QUERY_FIELD_NAME,
              true);
    }
    return sinkWrapper;
  }

  private CassandraSinkWrapper createWrapperForOutputFormat(Object object) {
    CassandraSinkWrapper sinkWrapper = null;
    if (isInstanceOf(object, CASSANDRA_POJO_OUTPUT_FORMAT_CLASS)) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              context.getUserClassLoader(),
              object,
              object.getClass(),
              POJO_OUTPUT_CLASS_FIELD_NAME,
              false);
    } else if (isInstanceOf(object, CASSANDRA_ROW_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, CASSANDRA_TUPLE_OUTPUT_FORMAT_CLASS)) {
      sinkWrapper =
          CassandraSinkWrapper.of(
              context.getUserClassLoader(),
              object,
              object.getClass(),
              INSERT_QUERY_FIELD_NAME,
              true);
    }
    return sinkWrapper;
  }

  private OpenLineage.OutputDataset getDataset(
      OpenLineageContext context, String namespace, String name) {
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
        CommonUtils.createSymlinkFacet(
            context.getOpenLineage(), Constants.CASSANDRA_TYPE, name, namespace);
    OpenLineage.DatasetFacets datasetFacets =
        outputDataset().getDatasetFacetsBuilder().symlinks(symlinksDatasetFacet).build();
    return openLineage
        .newOutputDatasetBuilder()
        .name(name)
        .namespace(namespace)
        .facets(datasetFacets)
        .build();
  }
}
