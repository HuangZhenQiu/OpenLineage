/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static io.openlineage.flink.utils.Constants.GENERIC_JDBC_SINK_FUNCTION_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_OUTPUT_FORMAT_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_XA_FINK_FUNCTION_CLASS;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.JdbcUtils;
import io.openlineage.flink.visitor.wrapper.JdbcSinkWrapper;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSinkVisitor extends Visitor<OpenLineage.OutputDataset> {
  public static final String JDBC_ROW_OUTPUT_FORMAT_CLASS =
      "org.apache.flink.connector.jdbc.JdbcRowOutputFormat";
  public static final String JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS =
      "org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction";

  public JdbcSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return isInstanceOf(object, JDBC_ROW_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, JDBC_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, GENERIC_JDBC_SINK_FUNCTION_CLASS)
        || isInstanceOf(object, JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS)
        || isInstanceOf(object, JDBC_XA_FINK_FUNCTION_CLASS);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object object) {
    log.debug("Apply sink {} in JdbcSinkVisitor", object);
    JdbcSinkWrapper sinkWrapper;
    if (isInstanceOf(object, JDBC_ROW_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, JDBC_OUTPUT_FORMAT_CLASS)
        || isInstanceOf(object, GENERIC_JDBC_SINK_FUNCTION_CLASS)
        || isInstanceOf(object, JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS)
        || isInstanceOf(object, JDBC_XA_FINK_FUNCTION_CLASS)) {
      sinkWrapper = JdbcSinkWrapper.of(object, context.getUserClassLoader());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported JDBC sink type %s", object.getClass().getCanonicalName()));
    }

    DatasetIdentifier di =
        JdbcUtils.getDatasetIdentifierFromJdbcUrl(
            sinkWrapper.getConnectionUrl().get(), sinkWrapper.getTableName().get());
    return Collections.singletonList(createOutputDataset(context, di.getNamespace(), di.getName()));
  }
}
