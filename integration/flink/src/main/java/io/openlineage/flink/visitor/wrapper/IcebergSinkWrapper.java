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
    try {
      Optional<Object> tableLoaderOpt = getTableLoader();
      Class tableLoaderClass = userClassLoader.loadClass("org.apache.iceberg.flink.TableLoader");
      if (tableLoaderOpt.isPresent()) {
        return WrapperUtils.invoke(tableLoaderClass, tableLoaderOpt.get(), "loadTable");
      }
    } catch (ClassNotFoundException e) {
      log.error("Can't find TableLoader class in classpath", e);
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
      log.warn("Failed extracting table loader from IcebergFilesCommitter", e);
    }

    return Optional.empty();
  }
}
