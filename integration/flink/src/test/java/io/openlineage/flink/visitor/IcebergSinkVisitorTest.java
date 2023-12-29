/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.utils.Constants;
import io.openlineage.flink.visitor.wrapper.IcebergSinkWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class IcebergSinkVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  IcebergSinkWrapper wrapper = mock(IcebergSinkWrapper.class);
  OneInputTransformation sink = mock(OneInputTransformation.class);
  OneInputStreamOperator icebergFilesCommitter = mock(OneInputStreamOperator.class);
  IcebergSinkVisitor sinkVisitor = new IcebergSinkVisitor(context);
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getUserClassLoader()).thenReturn(this.getClass().getClassLoader());
    when(sink.getOperator()).thenReturn(icebergFilesCommitter);
  }

  @Test
  void testIsDefinedOnNonIcebergSink() {
    assertFalse(sinkVisitor.isDefinedAt(mock(Object.class)));
  }

  @Test
  @SneakyThrows
  void testApply() {
    Table table = mock(Table.class, RETURNS_DEEP_STUBS);

    try (MockedStatic<IcebergSinkWrapper> mockedStatic = mockStatic(IcebergSinkWrapper.class)) {
      when(IcebergSinkWrapper.of(icebergFilesCommitter, context.getUserClassLoader()))
          .thenReturn(wrapper);
      when(table.location()).thenReturn("s3://bucket/table/");
      when(table.name()).thenReturn("hive.test.table");
      when(table.schema().columns())
          .thenReturn(
              Collections.singletonList(Types.NestedField.of(1, false, "a", Types.LongType.get())));
      doReturn(Optional.of(table)).when(wrapper).getTable();
      doReturn(Optional.of("thrift://localhost:9083")).when(wrapper).getNamespace();

      List<OutputDataset> outputDatasets = sinkVisitor.apply(sink);
      List<OpenLineage.SchemaDatasetFacetFields> fields =
          outputDatasets.get(0).getFacets().getSchema().getFields();

      assertEquals(1, outputDatasets.size());
      assertEquals("table", outputDatasets.get(0).getName());
      assertEquals("s3://bucket", outputDatasets.get(0).getNamespace());

      assertEquals(1, fields.size());
      assertEquals("a", fields.get(0).getName());
      assertEquals("LONG", fields.get(0).getType());

      List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinkIdentifiers =
          outputDatasets.get(0).getFacets().getSymlinks().getIdentifiers();
      Assertions.assertEquals(1, symlinkIdentifiers.size());

      OpenLineage.SymlinksDatasetFacetIdentifiers symlinkIdentifier =
          outputDatasets.get(0).getFacets().getSymlinks().getIdentifiers().get(0);
      Assertions.assertEquals(Constants.TABLE_TYPE, symlinkIdentifier.getType());
      Assertions.assertEquals("hive.test.table", symlinkIdentifier.getName());
      Assertions.assertEquals("thrift://localhost:9083", symlinkIdentifier.getNamespace());
    }
  }
}
