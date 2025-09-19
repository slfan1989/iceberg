/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Hive2Iceberg;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hive2IcebergAction extends BaseTableCreationSparkAction<Hive2IcebergAction>
    implements Hive2Iceberg {

  private static final Logger LOG = LoggerFactory.getLogger(Hive2IcebergAction.class);
  private SnapshotTableSparkAction snapshotTableSparkAction;
  private RewriteTablePathSparkAction rewriteTablePathSparkAction;
  private Identifier sourceTableIdent;
  private int parallelism = 1;
  private SparkSession sparkSession;
  private CatalogPlugin sourceCatalog;

  Hive2IcebergAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableIdent) {
    super(spark, sourceCatalog, sourceTableIdent);
    this.sourceTableIdent = sourceTableIdent;
    this.sparkSession = spark;
    this.sourceCatalog = sourceCatalog;
  }

  @Override
  public Hive2Iceberg parallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  @Override
  public Result execute() {
    /* Step1. Process SnapShot */
    snapshotTableSparkAction =
        new SnapshotTableSparkAction(sparkSession, sourceCatalog, sourceTableIdent);
    String snapShotTableName = generateSnapShotTableName(sourceTableIdent.name());
    snapshotTableSparkAction = snapshotTableSparkAction.as(snapShotTableName);
    snapshotTableSparkAction =
        snapshotTableSparkAction.executeWith(SparkTableUtil.migrationService(parallelism));
    Map<String, String> properties = Maps.newHashMap();
    SnapshotTable.Result result = snapshotTableSparkAction.tableProperties(properties).execute();
    long dataFilesCount = result.importedDataFilesCount();
    Preconditions.checkArgument(dataFilesCount > 0, "snapShot may have failed.");

    /* Step2. Process ReWrite */
    // Prepare SourceTable
    CatalogTable sourceSparkTable;
    Identifier sourceIdentifier;
    CatalogPlugin sourceCatalog;
    try {
      String ctx = "hive2iceberg rewrite source";
      sourceCatalog = sparkSession.sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(
              ctx, sparkSession, sourceTableIdent.name(), sourceCatalog);
      sourceIdentifier = catalogAndIdent.identifier();
      TableCatalog tableCatalog = checkTargetCatalog(catalogAndIdent.catalog());
      V1Table targetTable = (V1Table) tableCatalog.loadTable(sourceIdentifier);
      sourceSparkTable = targetTable.v1Table();
      LOG.info("hive2iceberg rewrite source table: {}", sourceSparkTable.qualifiedName());
    } catch (Exception e) {
      LOG.error("parse Source Table Error.", e);
      throw new RuntimeException(e);
    }

    // Prepare TargetTable
    SparkTable snapShotSparkTable;
    try {
      String ctx = "hive2iceberg target";
      CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
      Spark3Util.CatalogAndIdentifier catalogAndIdent =
          Spark3Util.catalogAndIdentifier(ctx, spark(), snapShotTableName, defaultCatalog);
      TableCatalog tableCatalog = checkTargetCatalog(catalogAndIdent.catalog());
      snapShotSparkTable = (SparkTable) tableCatalog.loadTable(catalogAndIdent.identifier());
      Table table = snapShotSparkTable.table();
      LOG.info("hive2iceberg dest table: {}", snapShotSparkTable.name());
    } catch (Exception e) {
      LOG.error("parse Dest Table Error.", e);
      throw new RuntimeException(e);
    }

    // 3. Rewrite Table Path
    String sourcePrefix = snapShotSparkTable.table().location();
    String targetPrefix = CatalogUtils.URIToString(sourceSparkTable.storage().locationUri().get());
    String stagingLocation = targetPrefix;
    boolean metaMigrate = true;

    RewriteTablePathSparkAction rewriteAction =
        SparkActions.get().rewriteTablePath(snapShotSparkTable.table());
    if (stagingLocation != null) {
      rewriteAction.stagingLocation(stagingLocation);
    }
    rewriteAction.hiveMigrate(true);
    RewriteTablePath.Result rewrite =
        rewriteAction.rewriteLocationPrefix(sourcePrefix, targetPrefix).execute();
    String fileListLocation = rewrite.fileListLocation();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileListLocation), "rewrite may have failed.");

    if (metaMigrate) {
      try {
        properties.put(CatalogProperties.CATALOG_IMPL, HiveCatalog.class.getName());

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark().sparkContext().hadoopConfiguration());
        hiveCatalog.initialize("hive", properties);

        String metadataFileLocation = rewrite.latestVersion();

        String targetMetadataFileLocation = targetPrefix + "/metadata/" + metadataFileLocation;
        TableCatalog targetSourceCatalog = checkTargetCatalog(sourceCatalog);
        targetSourceCatalog.alterTable(
            sourceIdentifier,
            TableChange.setProperty("write.parquet.compression-codec", "zstd"),
            TableChange.setProperty("dream.table_type.format", "iceberg"),
            TableChange.setProperty("hive_iceberg_change", "hivetoiceberg"),
            TableChange.setProperty("metadata_location", targetMetadataFileLocation),
            TableChange.setProperty("table_type", "ICEBERG"),
            TableChange.setProperty("provide", "iceberg"),
            TableChange.setProperty("gc.enabled", "true"));
      } catch (Exception e) {
        LOG.error("ReWrite metaMigrate Failed.", e);
        throw new RuntimeException(e);
      }
    }

    return null;
  }

  @Override
  protected TableCatalog checkSourceCatalog(CatalogPlugin catalog) {
    // currently the import code relies on being able to look up the table in the session catalog
    Preconditions.checkArgument(
        catalog.name().equalsIgnoreCase("spark_catalog"),
        "Cannot snapshot a table that isn't in the session catalog (i.e. spark_catalog). "
            + "Found source catalog: %s.",
        catalog.name());

    Preconditions.checkArgument(
        catalog instanceof TableCatalog,
        "Cannot snapshot as catalog %s of class %s in not a table catalog",
        catalog.name(),
        catalog.getClass().getName());

    return (TableCatalog) catalog;
  }

  @Override
  protected StagingTableCatalog destCatalog() {
    return null;
  }

  @Override
  protected Identifier destTableIdent() {
    return null;
  }

  @Override
  protected Map<String, String> destTableProps() {
    return Map.of();
  }

  @Override
  protected Hive2IcebergAction self() {
    return this;
  }

  /**
   * Generates a snapshot table name by appending the current timestamp to the given table name. The
   * timestamp is formatted as "yyyyMMddHHmmss" (year, month, day, hour, minute, second).
   *
   * @param tableName The base name of the table to which the snapshot suffix will be added.
   * @return The generated snapshot table name, which consists of the original table name followed
   *     by "_snapshot_" and the formatted timestamp.
   */
  private String generateSnapShotTableName(String tableName) {
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    String tableSuffix = now.format(formatter);
    String snapShotTableName = tableName + "_snapshot_" + tableSuffix;
    LOG.info("Snapshot table name: {}", snapShotTableName);
    return snapShotTableName;
  }

  private TableCatalog checkTargetCatalog(CatalogPlugin catalog) {
    // currently the import code relies on being able to look up the table in the session catalog
    Preconditions.checkArgument(
        catalog.name().equalsIgnoreCase("spark_catalog"),
        "Cannot rewrite a table that isn't in the session catalog (i.e. spark_catalog). "
            + "Found source catalog: %s.",
        catalog.name());

    Preconditions.checkArgument(
        catalog instanceof TableCatalog,
        "Cannot rewrite as catalog %s of class %s in not a table catalog",
        catalog.name(),
        catalog.getClass().getName());

    return (TableCatalog) catalog;
  }
}
