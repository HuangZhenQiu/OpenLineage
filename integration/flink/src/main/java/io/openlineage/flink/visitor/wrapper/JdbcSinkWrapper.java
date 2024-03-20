/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.Constants.GENERIC_JDBC_SINK_FUNCTION_CLASS;
import static io.openlineage.flink.utils.Constants.INTERNAL_JDBC_CONNECTION_OPTIONS_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_CONNECTION_OPTIONS_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_OUTPUT_FORMAT_CLASS;
import static io.openlineage.flink.utils.Constants.JDBC_XA_FINK_FUNCTION_CLASS;
import static io.openlineage.flink.utils.Constants.SIMPLE_JDBC_CONNECTION_PROVIDER_CLASS;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSinkWrapper {

  private Object sink;
  private ClassLoader userClassLoader;

  public <T> JdbcSinkWrapper(T sink, ClassLoader userClassLoader) {
    this.sink = sink;
    this.userClassLoader = userClassLoader;
  }

  public static <T> JdbcSinkWrapper of(T sink, ClassLoader userClassLoader) {
    return new JdbcSinkWrapper(sink, userClassLoader);
  }

  public Optional<String> getConnectionUrl() {
    try {
      Optional<Object> connectionOptionsOpt = getConnectionOptions();
      Class jdbcConnectionOptionsClass = Class.forName(JDBC_CONNECTION_OPTIONS_CLASS);
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
      Optional<Object> connectionOptionsOpt = getConnectionOptions();
      Class internalOptionsClass =
          userClassLoader.loadClass(INTERNAL_JDBC_CONNECTION_OPTIONS_CLASS);
      return connectionOptionsOpt
          .map(
              connectionOptions ->
                  WrapperUtils.<String>getFieldValue(
                      internalOptionsClass, connectionOptions, "tableName"))
          .orElse(Optional.of(""));
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the JDBC table name", e);
    }

    return Optional.of("");
  }

  private Optional<Object> getConnectionOptions() {
    try {
      Class jdbcOutputFormatClass = userClassLoader.loadClass(JDBC_OUTPUT_FORMAT_CLASS);
      Class genericJdbcSinkFunctionClass =
          userClassLoader.loadClass(GENERIC_JDBC_SINK_FUNCTION_CLASS);
      Class jdbcXaSinkFunctionClass = userClassLoader.loadClass(JDBC_XA_FINK_FUNCTION_CLASS);
      Class simpleJdbcConnectionProviderClass =
          userClassLoader.loadClass(SIMPLE_JDBC_CONNECTION_PROVIDER_CLASS);
      Optional jdbcOutputFormatOpt = Optional.empty();
      if (jdbcOutputFormatClass.isAssignableFrom(sink.getClass())) {
        jdbcOutputFormatOpt = Optional.of(sink);
      } else if (genericJdbcSinkFunctionClass.isAssignableFrom(sink.getClass())) {
        jdbcOutputFormatOpt =
            WrapperUtils.<Object>getFieldValue(genericJdbcSinkFunctionClass, sink, "outputFormat");
      } else if (jdbcXaSinkFunctionClass.isAssignableFrom(sink.getClass())) {
        jdbcOutputFormatOpt =
            WrapperUtils.<Object>getFieldValue(jdbcXaSinkFunctionClass, sink, "outputFormat");
      }

      if (jdbcOutputFormatOpt.isPresent()) {
        Optional<Object> providerOpt =
            WrapperUtils.<Object>getFieldValue(
                jdbcOutputFormatClass, jdbcOutputFormatOpt.get(), "connectionProvider");
        return providerOpt
            .map(
                provider ->
                    WrapperUtils.<Object>getFieldValue(
                        simpleJdbcConnectionProviderClass, provider, "jdbcOptions"))
            .get();
      }
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the JDBC connection options.", e);
    }

    return Optional.empty();
  }
}
