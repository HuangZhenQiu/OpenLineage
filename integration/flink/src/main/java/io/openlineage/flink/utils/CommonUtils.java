/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.client.OpenLineage;
import java.util.List;

public class CommonUtils {

  public static OpenLineage.SymlinksDatasetFacet createSymlinkFacet(
      OpenLineage openLineage, String type, String name, String namespace) {
    OpenLineage.SymlinksDatasetFacetIdentifiers identifier =
        new OpenLineage.SymlinksDatasetFacetIdentifiersBuilder()
            .type(type)
            .name(name)
            .namespace(namespace)
            .build();
    OpenLineage.SymlinksDatasetFacet symlinksDatasetFacet =
        openLineage.newSymlinksDatasetFacetBuilder().identifiers(List.of(identifier)).build();
    return symlinksDatasetFacet;
  }
}
