/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import io.openlineage.flink.utils.IcebergUtils;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergSinkWrapper {

  /** IcebergFilesCommitter */
  private final Object icebergFilesCommitter;

  private final ClassLoader userClassLoader;

  private IcebergSinkWrapper(Object icebergFilesCommitter, ClassLoader userClassLoader) {
    this.icebergFilesCommitter = icebergFilesCommitter;
    this.userClassLoader = userClassLoader;
  }

  public static IcebergSinkWrapper of(Object icebergFilesCommitter, ClassLoader userClassLoader) {
    return new IcebergSinkWrapper(icebergFilesCommitter, userClassLoader);
  }

  public Optional<Object> getTable() {
    Optional<Object> tableLoaderOpt = getTableLoader();
    if (tableLoaderOpt.isPresent()) {
      return WrapperUtils.invoke(
          tableLoaderOpt.get().getClass(), tableLoaderOpt.get(), "loadTable");
    }
    return Optional.empty();
  }

  public Optional<String> getNamespace() {
    Optional<Object> tableLoaderOpt = getTableLoader();
    return IcebergUtils.getNamespace(userClassLoader, tableLoaderOpt);
  }

  private Optional<Object> getTableLoader() {
    Class icebergFilesCommiterClass = null;
    try {
      icebergFilesCommiterClass =
          userClassLoader.loadClass("org.apache.iceberg.flink.sink.IcebergFilesCommitter");
      return WrapperUtils.<Object>getFieldValue(
          icebergFilesCommiterClass, icebergFilesCommitter, "tableLoader");
    } catch (ClassNotFoundException e) {
      log.error("Failed extracting table loader from IcebergFilesCommitter", e);
    }

    return Optional.empty();
  }
}
