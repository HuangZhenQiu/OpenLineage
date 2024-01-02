/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import lombok.Getter;

/** Configuration for {@link OpenLineageClient}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenLineageYaml {
  @Getter
  @JsonProperty("transport")
  private TransportConfig transportConfig;

  @Getter
  @JsonProperty("facets")
  private FacetsConfig facetsConfig;
}
