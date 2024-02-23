/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static io.openlineage.flink.utils.CassandraUtils.CASSANDRA_INPUT_FORMAT_BASE_CLASS;
import static org.apache.flink.util.Preconditions.checkState;

import io.openlineage.flink.utils.CassandraUtils;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraSourceWrapper<T> {

  private static final Pattern SELECT_REGEXP =
      Pattern.compile("(?i)select .+ from (\\w+)\\.(\\w+).*;$");
  private static final String POJO_CLASS_FIELD_NAME = "inputClass";
  private static final String QUERY_FIELD_NAME = "query";

  private ClassLoader userClassLoader;
  private T source;
  private Class sourceClass;
  private boolean hasQuery;

  public CassandraSourceWrapper(
      ClassLoader userClassLoader, T source, Class sourceClass, boolean hasQuery) {
    this.userClassLoader = userClassLoader;
    this.source = source;
    this.sourceClass = sourceClass;
    this.hasQuery = hasQuery;
  }

  public static <T> CassandraSourceWrapper of(
      ClassLoader userClassLoader, T source, Class sourceClass, boolean hasQuery) {
    return new CassandraSourceWrapper(userClassLoader, source, sourceClass, hasQuery);
  }

  public Optional<String> getNamespace() {
    try {
      Class inputFormatBase = userClassLoader.loadClass(CASSANDRA_INPUT_FORMAT_BASE_CLASS);
      Optional<Object> builderOpt = Optional.empty();

      if (inputFormatBase.isAssignableFrom(source.getClass())) {
        builderOpt = WrapperUtils.<Object>getFieldValue(inputFormatBase, source, "builder");
      }

      return CassandraUtils.findNamespaceFromBuilder(builderOpt, userClassLoader);
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the Cassandra namespace name", e);
    }

    return Optional.of("");
  }

  public Optional<String> getTableName() {
    if (hasQuery) {
      return Optional.of(String.join(".", extractFromQuery(1), extractFromQuery(2)));
    } else {
      Class pojoClass = getField(POJO_CLASS_FIELD_NAME);
      Optional<Object> tableOpt = CassandraUtils.extractTableAnnotation(pojoClass, userClassLoader);
      return CassandraUtils.findTableName(tableOpt, userClassLoader);
    }
  }

  private String extractFromQuery(int index) {
    String query = getField(QUERY_FIELD_NAME);
    final Matcher queryMatcher = SELECT_REGEXP.matcher(query);
    checkState(
        queryMatcher.matches(), "Query must be of the form select ... from keyspace.table ...;");
    return queryMatcher.group(index);
  }

  private <T> T getField(String name) {
    return WrapperUtils.<T>getFieldValue(sourceClass, source, name).get();
  }
}
