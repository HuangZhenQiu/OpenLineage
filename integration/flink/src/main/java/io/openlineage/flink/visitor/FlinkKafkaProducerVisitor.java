/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static io.openlineage.flink.utils.Constants.BOOTSTRAP_SERVER;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.AvroSchemaUtils;
import io.openlineage.flink.utils.CommonUtils;
import io.openlineage.flink.utils.Constants;
import io.openlineage.flink.utils.KafkaUtils;
import io.openlineage.flink.visitor.wrapper.FlinkKafkaProducerWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkKafkaProducerVisitor extends Visitor<OpenLineage.OutputDataset> {
  public FlinkKafkaProducerVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    return isInstanceOf(sink, "org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer");
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object flinkKafkaProducer) {
    FlinkKafkaProducerWrapper wrapper =
        FlinkKafkaProducerWrapper.of(flinkKafkaProducer, context.getUserClassLoader());
    Properties properties = wrapper.getKafkaProducerConfig();
    String topic = wrapper.getKafkaTopic();
    Optional<String> kaffeServersOpt =
        KafkaUtils.resolveBootstrapServerByKaffe(context.getUserClassLoader(), properties);
    String bootstrapServers =
        kaffeServersOpt.isPresent()
            ? kaffeServersOpt.get()
            : properties.getProperty(BOOTSTRAP_SERVER);

    String namespace = KafkaUtils.convertToNamespace(Optional.of(bootstrapServers));
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        outputDataset().getDatasetFacetsBuilder();

    OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
        CommonUtils.createSymlinkFacet(
            context.getOpenLineage(), Constants.KAFKA_TYPE, topic, namespace);

    wrapper
        .getAvroSchema()
        .map(
            schema ->
                datasetFacetsBuilder
                    .schema(AvroSchemaUtils.convert(context, schema))
                    .symlinks(symlinksDatasetFacet));

    log.info("Kafka output topic: {}", topic);

    return Collections.singletonList(
        outputDataset().getDataset(topic, bootstrapServers, datasetFacetsBuilder));
  }
}
