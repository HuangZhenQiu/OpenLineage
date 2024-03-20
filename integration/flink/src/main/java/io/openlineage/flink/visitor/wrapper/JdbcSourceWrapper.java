/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.Constants.JDBC_CONNECTION_OPTIONS_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_INPUT_FORMAT_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_ROW_DATA_INPUT_FORMAT_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS;
import static io.openlineage.flink.utils.Constants.SIMPLE_JDBC_CONNECTION_PROVIDER_CLASS;

import io.openlineage.sql.OpenLineageSql;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSourceWrapper {
  private Object source;
  private ClassLoader userClassLoader;

  public <T> JdbcSourceWrapper(T source, ClassLoader userClassLoader) {
    this.source = source;
    this.userClassLoader = userClassLoader;
  }

  public static <T> JdbcSourceWrapper of(T source, ClassLoader userClassLoader) {
    return new JdbcSourceWrapper(source, userClassLoader);
  }

  public Optional<String> getConnectionUrl() {
    try {
      Optional<Object> connectionOptionsOpt = getConnectionOptions();
      Class jdbcConnectionOptionsClass = userClassLoader.loadClass(JDBC_CONNECTION_OPTIONS_CLASS);
      return connectionOptionsOpt
          .map(
              connectionOptions ->
                  WrapperUtils.<String>invoke(
                      jdbcConnectionOptionsClass, connectionOptions, "getDbURL"))
          .orElseThrow();
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the JDBC connection url", e);
    }

    return Optional.of("");
  }

  public Optional<String> getTableName() {
    try {
      Class jdbcInputFormatClass = userClassLoader.loadClass(JDBC_INPUT_FORMAT_CLASS);
      Class jdbcRowDataInputFormatClass =
          userClassLoader.loadClass(JDBC_ROW_DATA_INPUT_FORMAT_CLASS);
      Class jdbcRowDataLookupFunctionClass =
          userClassLoader.loadClass(JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS);
      Optional<String> queryOpt = Optional.empty();
      if (jdbcRowDataLookupFunctionClass.isAssignableFrom(source.getClass())) {
        Optional<Object> connectionOptionsOpt = getConnectionOptions();
        return connectionOptionsOpt
            .map(
                connectionOptions ->
                    WrapperUtils.<String>getFieldValue(
                        connectionOptions.getClass(), connectionOptions, "tableName"))
            .orElse(Optional.of(""));
      } else if (jdbcInputFormatClass.isAssignableFrom(source.getClass())) {
        queryOpt =
            WrapperUtils.<String>getFieldValue(jdbcInputFormatClass, source, "queryTemplate");
      } else if (jdbcRowDataLookupFunctionClass.isAssignableFrom(source.getClass())) {
        queryOpt =
            WrapperUtils.<String>getFieldValue(
                jdbcRowDataInputFormatClass, source, "queryTemplate");
      }

      return queryOpt
          .flatMap(query -> OpenLineageSql.parse(List.of(query)))
          .map(sqlMeta -> sqlMeta.inTables().isEmpty() ? "" : sqlMeta.inTables().get(0).name());
    } catch (ClassNotFoundException e) {

    }

    return Optional.of("");
  }

  private Optional<Object> getConnectionOptions() {

    try {
      Class jdbcInputFormatClass = userClassLoader.loadClass(JDBC_INPUT_FORMAT_CLASS);
      Class jdbcRowDataInputFormatClass =
          userClassLoader.loadClass(JDBC_ROW_DATA_INPUT_FORMAT_CLASS);
      Class jdbcRowDataLookupFunctionClass =
          userClassLoader.loadClass(JDBC_ROW_DATA_LOOKUP_FUNCTION_CLASS);
      Class simpleJdbcConnectionProviderClass =
          userClassLoader.loadClass(SIMPLE_JDBC_CONNECTION_PROVIDER_CLASS);

      if (jdbcInputFormatClass.isAssignableFrom(source.getClass())
          || jdbcRowDataInputFormatClass.isAssignableFrom(source.getClass())
          || jdbcRowDataLookupFunctionClass.isAssignableFrom(source.getClass())) {
        Optional<Object> providerOpt =
            WrapperUtils.<Object>getFieldValue(source.getClass(), source, "connectionProvider");
        return providerOpt
            .map(
                provider ->
                    WrapperUtils.<Object>getFieldValue(
                        simpleJdbcConnectionProviderClass, provider, "jdbcOptions"))
            .get();
      }
    } catch (ClassNotFoundException e) {

    }

    return Optional.empty();
  }
}
