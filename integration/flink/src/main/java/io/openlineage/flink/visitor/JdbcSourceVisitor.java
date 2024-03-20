/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static io.openlineage.flink.utils.Constants.JDBC_INPUT_FORMAT_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_ROW_DATA_INPUT_FORMAT_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.JdbcUtils;
import io.openlineage.flink.visitor.wrapper.JdbcSourceWrapper;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSourceVisitor extends Visitor<OpenLineage.InputDataset> {

  public JdbcSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return isInstanceOf(object, JDBC_INPUT_FORMAT_CLASS)
        || isInstanceOf(object, JDBC_ROW_DATA_INPUT_FORMAT_CLASS)
        || isInstanceOf(object, JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    log.debug("Apply source {} in JdbcSourceVisitor", object);
    JdbcSourceWrapper sourceWrapper;
    if (isInstanceOf(object, JDBC_INPUT_FORMAT_CLASS)
        || isInstanceOf(object, JDBC_ROW_DATA_INPUT_FORMAT_CLASS)
        || isInstanceOf(object, JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS)) {
      sourceWrapper = JdbcSourceWrapper.of(object, context.getUserClassLoader());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported JDBC Source type %s", object.getClass().getCanonicalName()));
    }

    DatasetIdentifier di =
        JdbcUtils.getDatasetIdentifierFromJdbcUrl(
            sourceWrapper.getConnectionUrl().get(), sourceWrapper.getTableName().get());
    return Collections.singletonList(createInputDataset(context, di.getNamespace(), di.getName()));
  }
}
