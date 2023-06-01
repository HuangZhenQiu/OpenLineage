/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, OpenLineage.OutputDataset> {

  private static final Logger logger =
      LoggerFactory.getLogger(InsertIntoHadoopFsRelationVisitor.class);

  public InsertIntoHadoopFsRelationVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;
    OpenLineage.OutputDataset outputDataset;
    DatasetIdentifier di;
    StructType schema;
    if (command.catalogTable().isDefined()) {
      CatalogTable catalogTable = command.catalogTable().get();
      di = PathUtils.fromCatalogTable(catalogTable);
      schema = catalogTable.schema();
    } else {
      di = PathUtils.fromURI(command.outputPath().toUri(), "file");
      schema = command.query().schema();
    }

    if (SaveMode.Overwrite == command.mode()) {
      outputDataset =
          outputDataset()
              .getDataset(
                  di,
                  schema,
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    } else {
      outputDataset = outputDataset().getDataset(di, command.query().schema());
    }

    return Collections.singletonList(outputDataset);
  }
}
