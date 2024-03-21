/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HybridSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public static final String HYBRID_SOURCE_CLASS =
      "org.apache.flink.connector.base.source.hybrid.HybridSource";
  public static final String HYBRID_SOURCE_FACTORY_CLASS =
      "org.apache.flink.connector.base.source.hybrid.HybridSource.SourceFactory";
  public static final String SOURCE_LIST_ENTRY_CLASS =
      "org.apache.flink.connector.base.source.hybrid.HybridSource$SourceListEntry";
  public static final String PASS_THROUGH_SOURCE_FACTORY_CLASS =
      "org.apache.flink.connector.base.source.hybrid.HybridSource$PassthroughSourceFactory";
  private VisitorFactoryImpl visitorFactory = new VisitorFactoryImpl();

  public HybridSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return isInstanceOf(object, HYBRID_SOURCE_CLASS);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    log.debug("Apply source {} in HybridSourceVisitor", object);
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    try {
      if (isInstanceOf(object, HYBRID_SOURCE_CLASS)) {
        Class sourceListEntryClass =
            context.getUserClassLoader().loadClass(SOURCE_LIST_ENTRY_CLASS);
        Optional<List<Object>> sourceListOpt =
            WrapperUtils.<List<Object>>getFieldValue(object.getClass(), object, "sources");
        if (sourceListOpt.isPresent()) {
          List<Object> sourceList = sourceListOpt.get();
          for (Object sourceEntry : sourceList) {
            Optional<Object> sourceFactoryOpt =
                WrapperUtils.<Object>getFieldValue(sourceListEntryClass, sourceEntry, "factory");
            if (sourceListOpt.isPresent()) {
              Class passThroughSourceFactory =
                  context.getUserClassLoader().loadClass(PASS_THROUGH_SOURCE_FACTORY_CLASS);
              Optional<Object> sourceOpt =
                  WrapperUtils.<Object>getFieldValue(
                      passThroughSourceFactory, sourceFactoryOpt.get(), "source");
              if (sourceOpt.isPresent()) {
                Object source = sourceOpt.get();
                visitorFactory.getInputVisitors(context).stream()
                    .filter(visitor -> visitor.isDefinedAt(source))
                    .forEach(visitor -> inputDatasets.addAll(visitor.apply(source)));
              }
            }
          }
        }
      }
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to find the sources added into Hybrid Source", e);
    }

    return inputDatasets;
  }
}
