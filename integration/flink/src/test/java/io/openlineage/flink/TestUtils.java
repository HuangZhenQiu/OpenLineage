/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import com.datastax.driver.core.Cluster;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class TestUtils {

  public static ClusterBuilder createClusterBuilder(String contactPoints) {
    return new ClusterBuilder() {
      @Override
      protected Cluster buildCluster(Cluster.Builder builder) {
        return builder.addContactPoint(contactPoints).build();
      }
    };
  }
}
