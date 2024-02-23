/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.CassandraUtils.CASSANDRA_OUTPUT_FORMAT_BASE_CLASS;
import static io.openlineage.flink.utils.CassandraUtils.CASSANDRA_SINK_BASE_CLASS;
import static org.apache.flink.util.Preconditions.checkState;

import io.openlineage.flink.utils.CassandraUtils;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraSinkWrapper<T> {
  public static final String POJO_OUTPUT_CLASS_FIELD_NAME = "outputClass";
  public static final String POJO_CLASS_FIELD_NAME = "clazz";
  public static final String INSERT_QUERY_FIELD_NAME = "insertQuery";

  private static final Pattern INSERT_REGEXP =
      Pattern.compile("(?i)insert.+into (\\w+)\\.(\\w+).*;$");
  private ClassLoader userClassLoader;
  private String fieldName;
  private T sink;
  private Class sinkClass;
  private boolean hasInsertQuery;

  public static <T> CassandraSinkWrapper of(
      ClassLoader userClassLoader,
      T sink,
      Class sinkClass,
      String fieldName,
      boolean hasInsertQuery) {
    return new CassandraSinkWrapper(userClassLoader, sink, sinkClass, fieldName, hasInsertQuery);
  }

  public CassandraSinkWrapper(
      ClassLoader userClassLoader,
      T sink,
      Class sinkClass,
      String fieldName,
      boolean hasInsertQuery) {
    this.userClassLoader = userClassLoader;
    this.sink = sink;
    this.sinkClass = sinkClass;
    this.hasInsertQuery = hasInsertQuery;
    this.fieldName = fieldName;
  }

  public Optional<String> getNamespace() {
    try {
      Class outputFormatBase = userClassLoader.loadClass(CASSANDRA_OUTPUT_FORMAT_BASE_CLASS);
      Class sinkBase = userClassLoader.loadClass(CASSANDRA_SINK_BASE_CLASS);
      Optional<Object> builderOpt = Optional.empty();

      if (outputFormatBase.isAssignableFrom(sink.getClass())) {
        builderOpt = WrapperUtils.<Object>getFieldValue(outputFormatBase, sink, "builder");
      } else if (sinkBase.isAssignableFrom(sink.getClass())) {
        builderOpt = WrapperUtils.<Object>getFieldValue(sinkBase, sink, "builder");
      }

      return CassandraUtils.findNamespaceFromBuilder(builderOpt, userClassLoader);
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the Cassandra namespace name", e);
    }

    return Optional.of("");
  }

  public Optional<String> getTableName() {
    if (hasInsertQuery) {
      return Optional.of(String.join(".", extractFromQuery(1), extractFromQuery(2)));
    } else {
      Class pojoClass = getField(fieldName);
      Optional<Object> tableOpt = CassandraUtils.extractTableAnnotation(pojoClass, userClassLoader);
      return CassandraUtils.findTableName(tableOpt, userClassLoader);
    }
  }

  private String extractFromQuery(int index) {
    String query = getField(fieldName);
    final Matcher queryMatcher = INSERT_REGEXP.matcher(query);
    checkState(
        queryMatcher.matches(), "Insert query must be of the form insert into keyspace.table ...;");
    return queryMatcher.group(index);
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(sinkClass, sink, name).get();
  }
}
