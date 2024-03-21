/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class CassandraUtils {
  public static final String CASSANDRA_INPUT_FORMAT_BASE_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraInputFormatBase";
  public static final String CASSANDRA_OUTPUT_FORMAT_BASE_CLASS =
      "org.apache.flink.batch.connectors.cassandra.CassandraOutputFormatBase";
  public static final String CASSANDRA_SINK_BASE_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.CassandraSinkBase";
  public static final String CASSANDRA_CLUSTER_CLASS = "com.datastax.driver.core.Cluster";
  public static final String CASSANDRA_CLUSTER_BUILDER_CLASS =
      "org.apache.flink.streaming.connectors.cassandra.ClusterBuilder";
  public static final String CASSANDRA_MANAGER_CLASS = "com.datastax.driver.core.Cluster$Manager";
  public static final String CASSANDRA_TABLE_ANNOTATION_CLASS =
      "com.datastax.driver.mapping.annotations.Table";

  public static Optional<String> convertToNamespace(Optional<List<Object>> endpointsOpt) {
    if (endpointsOpt.isPresent() && !endpointsOpt.isEmpty()) {
      return Optional.of("cassandra://" + endpointsOpt.get().get(0).toString().split("/")[1]);
    }

    return Optional.of("");
  }

  public static Optional<String> findNamespaceFromBuilder(
      Optional<Object> builderOpt, ClassLoader userClassLoader) {

    try {
      if (builderOpt.isPresent()) {
        Class clusterBuilder = userClassLoader.loadClass(CASSANDRA_CLUSTER_BUILDER_CLASS);
        Class clusterClass = userClassLoader.loadClass(CASSANDRA_CLUSTER_CLASS);
        Class managerClass = userClassLoader.loadClass(CASSANDRA_MANAGER_CLASS);

        Optional<Object> clusterOpt =
            WrapperUtils.invoke(clusterBuilder, builderOpt.get(), "getCluster");
        if (clusterOpt.isPresent()) {
          Optional<Object> managerOpt =
              WrapperUtils.<Object>getFieldValue(clusterClass, clusterOpt.get(), "manager");
          if (managerOpt.isPresent()) {
            Optional<List<Object>> endpointsOpt =
                WrapperUtils.<List<Object>>getFieldValue(
                    managerClass, managerOpt.get(), "contactPoints");
            return CassandraUtils.convertToNamespace(endpointsOpt);
          }
        }
      }
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the Cassandra namespace name", e);
    }
    return Optional.of("");
  }

  public static Optional<Object> extractTableAnnotation(Class pojo, ClassLoader userClassLoader) {
    try {
      Class tableAnnotation = userClassLoader.loadClass(CASSANDRA_TABLE_ANNOTATION_CLASS);
      Annotation[] annotations = pojo.getAnnotations();
      for (Annotation annotation : annotations) {
        if (tableAnnotation.isAssignableFrom(annotation.getClass())) {
          return Optional.of((Object) annotation);
        }
      }
    } catch (ClassNotFoundException e) {
      log.error(
          "Failed load class table annotation required to infer the Cassandra namespace name", e);
    }

    return Optional.empty();
  }

  public static Optional<String> findTableName(
      Optional<Object> tableOpt, ClassLoader userClassLoader) {
    try {
      Class tableAnnotationClass = userClassLoader.loadClass(CASSANDRA_TABLE_ANNOTATION_CLASS);
      String keyspace =
          WrapperUtils.<String>invoke(tableAnnotationClass, tableOpt.get(), "keyspace").orElse("");
      String name =
          WrapperUtils.<String>invoke(tableAnnotationClass, tableOpt.get(), "name").orElse("");
      if (!StringUtils.isBlank(keyspace) && !StringUtils.isBlank(name)) {
        return Optional.of(String.join(".", keyspace, name));
      }
    } catch (ClassNotFoundException e) {
      log.error(
          "Failed load class table annotation required to infer the Cassandra namespace name", e);
    }
    return Optional.of("");
  }
}
