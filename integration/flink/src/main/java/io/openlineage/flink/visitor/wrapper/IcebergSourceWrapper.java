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
  private ClassLoader userClassLoader;

  public IcebergSourceWrapper(T source, ClassLoader userClassLoader) {
    this.source = source;
    this.userClassLoader = userClassLoader;
  }

  public static <T> IcebergSourceWrapper of(T source, ClassLoader userClassLoader) {
    return new IcebergSourceWrapper(source, userClassLoader);
  }

  public Optional<Object> getTable() {
    Optional<Object> tableLoaderOpt =
        WrapperUtils.<Object>getFieldValue(source.getClass(), source, "tableLoader");
    if (tableLoaderOpt.isPresent()) {
      return WrapperUtils.invoke(
          tableLoaderOpt.get().getClass(), tableLoaderOpt.get(), "loadTable");
    }

    return Optional.empty();
  }

  public Optional<String> getNamespace() {
    return IcebergUtils.getNamespace(
        userClassLoader, WrapperUtils.getFieldValue(source.getClass(), source, "tableLoader"));
  }
}
