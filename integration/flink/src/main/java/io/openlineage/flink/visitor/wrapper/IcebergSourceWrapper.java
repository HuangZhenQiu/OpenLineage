/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import io.openlineage.flink.utils.IcebergUtils;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergSourceWrapper<T> {

  private final T source;
  private Class sourceClass;
  private ClassLoader userClassLoader;

  public IcebergSourceWrapper(T source, Class sourceClass, ClassLoader userClassLoader) {
    this.source = source;
    this.sourceClass = sourceClass;
    this.userClassLoader = userClassLoader;
  }

  public static <T> IcebergSourceWrapper of(
      T source, Class sourceClass, ClassLoader userClassLoader) {
    return new IcebergSourceWrapper(source, sourceClass, userClassLoader);
  }

  public Optional<Object> getTable() {
    try {
      Optional<Object> tableLoaderOpt =
          WrapperUtils.<Object>getFieldValue(sourceClass, source, "tableLoader");
      if (tableLoaderOpt.isPresent()) {
        Class tableLoaderClass = userClassLoader.loadClass("org.apache.iceberg.flink.TableLoader");
        return WrapperUtils.invoke(tableLoaderClass, tableLoaderOpt.get(), "loadTable");
      }

    } catch (ClassNotFoundException e) {
      log.error("Iceberg TableLoader class is not found", e);
    }
    return Optional.empty();
  }

  public Optional<String> getNamespace() {
    return IcebergUtils.getNamespace(
        userClassLoader, WrapperUtils.getFieldValue(sourceClass, source, "tableLoader"));
  }
}
