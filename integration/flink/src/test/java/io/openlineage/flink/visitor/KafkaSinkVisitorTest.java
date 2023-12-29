/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.Constants;
import io.openlineage.flink.visitor.wrapper.KafkaSinkWrapper;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class KafkaSinkVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  KafkaSinkVisitor visitor = new KafkaSinkVisitor(context);
  KafkaSink kafkaSink = mock(KafkaSink.class);
  Properties props = new Properties();
  KafkaSinkWrapper wrapper = mock(KafkaSinkWrapper.class);
  OpenLineage openLineage = new OpenLineage(mock(URI.class));
  Schema schema =
      SchemaBuilder.record("OutputEvent")
          .namespace("io.openlineage.flink.avro.event")
          .fields()
          .name("a")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  @BeforeEach
  @SneakyThrows
  public void setup() {
    props.put("bootstrap.servers", "server1;server2");
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getUserClassLoader()).thenReturn(this.getClass().getClassLoader());
  }

  @Test
  void testIsDefined() {
    assertFalse(visitor.isDefinedAt(mock(Object.class)));
    assertTrue(visitor.isDefinedAt(mock(KafkaSink.class)));
  }

  @Test
  @SneakyThrows
  void testApply() {
    try (MockedStatic<KafkaSinkWrapper> mockedStatic = mockStatic(KafkaSinkWrapper.class)) {
      when(KafkaSinkWrapper.of(kafkaSink, context.getUserClassLoader())).thenReturn(wrapper);

      when(wrapper.getKafkaTopic()).thenReturn("topic");
      when(wrapper.getKafkaProducerConfig()).thenReturn(props);
      when(wrapper.getAvroSchema()).thenReturn(Optional.of(schema));

      OpenLineage.OutputDataset outputDataset = visitor.apply(kafkaSink).get(0);
      List<OpenLineage.SchemaDatasetFacetFields> fields =
          outputDataset.getFacets().getSchema().getFields();

      assertEquals("topic", outputDataset.getName());
      assertEquals("server1;server2", outputDataset.getNamespace());

      assertEquals(1, fields.size());
      assertEquals("a", fields.get(0).getName());
      assertEquals("long", fields.get(0).getType());

      List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinkIdentifiers =
          outputDataset.getFacets().getSymlinks().getIdentifiers();
      assertEquals(1, symlinkIdentifiers.size());

      OpenLineage.SymlinksDatasetFacetIdentifiers symlinkIdentifier =
          outputDataset.getFacets().getSymlinks().getIdentifiers().get(0);
      assertEquals(Constants.KAFKA_TYPE, symlinkIdentifier.getType());
      assertEquals("topic", symlinkIdentifier.getName());
      assertEquals("server1;server2", symlinkIdentifier.getNamespace());
    }
  }

  @Test
  @SneakyThrows
  void testApplyWhenIllegalAccessExceptionThrown() {
    try (MockedStatic<KafkaSinkWrapper> mockedStatic = mockStatic(KafkaSinkWrapper.class)) {
      when(KafkaSinkWrapper.of(kafkaSink, context.getUserClassLoader())).thenReturn(wrapper);

      when(wrapper.getKafkaProducerConfig()).thenReturn(props);
      when(wrapper.getKafkaTopic()).thenThrow(new IllegalAccessException(""));
      List<OpenLineage.OutputDataset> outputDatasets = visitor.apply(kafkaSink);

      assertEquals(0, outputDatasets.size());
    }
  }
}
