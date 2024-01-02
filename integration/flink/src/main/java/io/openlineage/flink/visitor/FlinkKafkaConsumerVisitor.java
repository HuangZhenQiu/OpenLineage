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
import io.openlineage.flink.visitor.wrapper.FlinkKafkaConsumerWrapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkKafkaConsumerVisitor extends Visitor<OpenLineage.InputDataset> {

  public FlinkKafkaConsumerVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return isInstanceOf(object, "org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer");
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    try {
      FlinkKafkaConsumerWrapper wrapper =
          FlinkKafkaConsumerWrapper.of(object, context.getUserClassLoader());
      Properties properties = wrapper.getKafkaProperties();
      Optional<String> kaffeServersOpt =
          KafkaUtils.resolveBootstrapServerByKaffe(context.getUserClassLoader(), properties);
      String bootstrapServers =
          kaffeServersOpt.isPresent()
              ? kaffeServersOpt.get()
              : properties.getProperty(BOOTSTRAP_SERVER);

      OpenLineage openLineage = context.getOpenLineage();

      return wrapper.getTopics().stream()
          .map(
              topic -> {
                String namespace = KafkaUtils.convertToNamespace(Optional.of(bootstrapServers));
                OpenLineage.InputDatasetBuilder builder =
                    openLineage.newInputDatasetBuilder().namespace(namespace).name(topic);

                OpenLineage.DatasetFacetsBuilder facetsBuilder =
                    inputDataset().getDatasetFacetsBuilder();

                OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
                    CommonUtils.createSymlinkFacet(
                        context.getOpenLineage(), Constants.KAFKA_TYPE, topic, namespace);

                wrapper
                    .getAvroSchema()
                    .ifPresent(
                        schema ->
                            facetsBuilder
                                .schema(AvroSchemaUtils.convert(context, schema))
                                .symlinks(symlinksDatasetFacet));

                return builder.facets(facetsBuilder.build()).build();
              })
          .collect(Collectors.toList());
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
