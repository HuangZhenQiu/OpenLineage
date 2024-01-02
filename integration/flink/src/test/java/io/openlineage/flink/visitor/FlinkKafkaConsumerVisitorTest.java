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
import io.openlineage.flink.visitor.wrapper.FlinkKafkaConsumerWrapper;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class FlinkKafkaConsumerVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  FlinkKafkaConsumer flinkKafkaConsumer = mock(FlinkKafkaConsumer.class);
  FlinkKafkaConsumerVisitor flinkKafkaConsumerVisitor = new FlinkKafkaConsumerVisitor(context);
  FlinkKafkaConsumerWrapper wrapper = mock(FlinkKafkaConsumerWrapper.class);
  Properties props = new Properties();
  OpenLineage openLineage = new OpenLineage(mock(URI.class));
  Schema schema =
      SchemaBuilder.record("InputEvent")
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
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getUserClassLoader()).thenReturn(this.getClass().getClassLoader());
  }

  @Test
  void testIsDefined() {
    assertFalse(flinkKafkaConsumerVisitor.isDefinedAt(mock(Object.class)));
    assertTrue(flinkKafkaConsumerVisitor.isDefinedAt(mock(FlinkKafkaConsumer.class)));
  }

  @Test
  @SneakyThrows
  void testApply() {
    props.put("bootstrap.servers", "server1:65506,server2:65506");

    try (MockedStatic<FlinkKafkaConsumerWrapper> mockedStatic =
        mockStatic(FlinkKafkaConsumerWrapper.class)) {
      when(FlinkKafkaConsumerWrapper.of(
              flinkKafkaConsumer, flinkKafkaConsumer.getClass().getClassLoader()))
          .thenReturn(wrapper);

      when(wrapper.getTopics()).thenReturn(Arrays.asList("topic1", "topic2"));
      when(wrapper.getKafkaProperties()).thenReturn(props);
      when(wrapper.getAvroSchema()).thenReturn(Optional.of(schema));

      List<OpenLineage.InputDataset> inputDatasets =
          flinkKafkaConsumerVisitor.apply(flinkKafkaConsumer);
      List<OpenLineage.SchemaDatasetFacetFields> fields =
          inputDatasets.get(0).getFacets().getSchema().getFields();

      assertEquals(2, inputDatasets.size());
      assertEquals("topic1", inputDatasets.get(0).getName());
      assertEquals("kafka://server1:65506", inputDatasets.get(0).getNamespace());

      assertEquals(1, fields.size());
      assertEquals("a", fields.get(0).getName());
      assertEquals("long", fields.get(0).getType());

      List<OpenLineage.SymlinksDatasetFacetIdentifiers> symlinkIdentifiers =
          inputDatasets.get(0).getFacets().getSymlinks().getIdentifiers();
      assertEquals(1, symlinkIdentifiers.size());

      OpenLineage.SymlinksDatasetFacetIdentifiers symlinkIdentifier =
          inputDatasets.get(0).getFacets().getSymlinks().getIdentifiers().get(0);
      assertEquals(Constants.KAFKA_TYPE, symlinkIdentifier.getType());
      assertEquals("topic1", symlinkIdentifier.getName());
      assertEquals("kafka://server1:65506", symlinkIdentifier.getNamespace());
    }
  }

  @Test
  @SneakyThrows
  void testApplyWhenIllegalAccessExceptionThrown() {
    try (MockedStatic<FlinkKafkaConsumerWrapper> mockedStatic =
        mockStatic(FlinkKafkaConsumerWrapper.class)) {
      when(FlinkKafkaConsumerWrapper.of(
              flinkKafkaConsumer, flinkKafkaConsumer.getClass().getClassLoader()))
          .thenReturn(wrapper);

      when(wrapper.getKafkaProperties()).thenReturn(props);
      when(wrapper.getTopics()).thenThrow(new IllegalAccessException(""));
      List<OpenLineage.InputDataset> inputDatasets =
          flinkKafkaConsumerVisitor.apply(flinkKafkaConsumer);

      assertEquals(0, inputDatasets.size());
    }
  }
}
