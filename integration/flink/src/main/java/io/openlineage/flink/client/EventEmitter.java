/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class EventEmitter {

  private final OpenLineageClient client;

  public static final URI OPEN_LINEAGE_CLIENT_URI = getUri();
  public static final String OPEN_LINEAGE_PARENT_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/ParentRunFacet";
  public static final String OPEN_LINEAGE_DATASOURCE_FACET =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/DatasourceDatasetFacet";
  public static final String OPEN_LINEAGE_SCHEMA_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/SchemaDatasetFacet";
  public static final String DEFAULT_TRANSPORT_TYPE = "http";
  public static final String OPEN_LINEAGE_TRANSPORT_TYPE = "openlineage.transport.type";
  public static final String OPEN_LINEAGE_TRANSPORT_URL = "openlineage.transport.url";
  public static final String OPEN_LINEAGE_TRANSPORT_ENDPOINT = "openlineage.transport.endpoint";

  @VisibleForTesting
  public EventEmitter(OpenLineageClient client) {
    this.client = client;
  }

  public EventEmitter(Configuration configuration) {
    String type = configuration.getString(OPEN_LINEAGE_TRANSPORT_TYPE, "");
    String url = configuration.getString(OPEN_LINEAGE_TRANSPORT_URL, "");
    String endpoint = configuration.getString(OPEN_LINEAGE_TRANSPORT_ENDPOINT, "");
    if (!StringUtils.isBlank(type)
        && !StringUtils.isBlank(url)
        && !StringUtils.isBlank(endpoint)
        && type.equals(DEFAULT_TRANSPORT_TYPE)) {
      // build emitter client based on flink configuration
      HttpConfig httpConfig = new HttpConfig();
      httpConfig.setEndpoint(endpoint);
      httpConfig.setUrl(URI.create(url));
      this.client = OpenLineageClient.builder().transport(new HttpTransport(httpConfig)).build();
    } else {
      // build emitter default way - openlineage.yml file or system properties
      client = Clients.newClient();
    }
  }

  public void emit(OpenLineage.RunEvent event) {
    try {
      client.emit(event);
    } catch (OpenLineageClientException exception) {
      log.error("Failed to emit OpenLineage event: ", exception);
    }
  }

  private static URI getUri() {
    return URI.create(
        String.format(
            "https://github.com/OpenLineage/OpenLineage/tree/%s/integration/flink", getVersion()));
  }

  private static String getVersion() {
    try {
      Properties properties = new Properties();
      InputStream is = EventEmitter.class.getResourceAsStream("version.properties");
      properties.load(is);
      return properties.getProperty("version");
    } catch (IOException exception) {
      return "main";
    }
  }
}
