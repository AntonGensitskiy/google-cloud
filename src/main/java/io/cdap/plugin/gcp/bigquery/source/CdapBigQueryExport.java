package io.cdap.plugin.gcp.bigquery.source;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.io.bigquery.BigQueryUtils;
import com.google.cloud.hadoop.io.bigquery.Export;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.io.bigquery.ShardedExportToCloudStorage;
import com.google.cloud.hadoop.io.bigquery.UnshardedExportToCloudStorage;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * A Export decorator that will attempt to perform a query during the export prepare phase.
 */
public class CdapBigQueryExport implements Export {
  private static final Logger LOG = LoggerFactory.getLogger(CdapBigQueryExport.class);

  private final Configuration configuration;
  private final String gcsPath;
  private final ExportFileFormat fileFormat;
  private final String query;
  private final BigQueryHelper bigQueryHelper;
  private final String projectId;
  private final TableReference tableToExport;
  private final boolean deleteIntermediateTable;
  private final boolean enableShardedExport;
  private Export delegate;

  public CdapBigQueryExport(Configuration configuration, String gcsPath, ExportFileFormat fileFormat, String query,
                            String projectId, BigQueryHelper bigQueryHelper, TableReference tableToExport,
                            boolean deleteIntermediateTable, boolean enableShardedExport) {
    this.configuration = configuration;
    this.gcsPath = gcsPath;
    this.fileFormat = fileFormat;
    this.query = query;
    this.bigQueryHelper = bigQueryHelper;
    this.projectId = projectId;
    this.tableToExport = tableToExport;
    this.deleteIntermediateTable = deleteIntermediateTable;
    this.enableShardedExport = enableShardedExport;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return delegate.getSplits(context);
  }

  @Override
  public List<String> getExportPaths() throws IOException {
    return delegate.getExportPaths();
  }

  @Override
  public void beginExport() throws IOException {
    delegate.beginExport();
  }

  @Override
  public void waitForUsableMapReduceInput() throws IOException, InterruptedException {
    delegate.waitForUsableMapReduceInput();
  }

  @Override
  public void prepare() throws IOException {
    if (!Strings.isNullOrEmpty(query)) {
      try {
        runQuery(bigQueryHelper, projectId, tableToExport, query);
      } catch (InterruptedException ie) {
        throw new IOException(
          String.format("Interrupted during query '%s' into table '%s'",
                        query, BigQueryStrings.toString(tableToExport)), ie);
      }
    }

    Table table = bigQueryHelper.getTable(tableToExport);

    if (enableShardedExport) {
      delegate = new ShardedExportToCloudStorage(
        configuration,
        gcsPath,
        fileFormat,
        bigQueryHelper,
        projectId,
        table);
    } else {
      delegate = new UnshardedExportToCloudStorage(
        configuration,
        gcsPath,
        fileFormat,
        bigQueryHelper,
        projectId,
        table,
        null);
    }
    delegate.prepare();
  }

  private static void runQuery(
    BigQueryHelper bigQueryHelper, String projectId, TableReference tableRef, String query)
    throws IOException, InterruptedException {

    // Create a query statement and query request object.
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    queryConfig.setAllowLargeResults(true);
    queryConfig.setQuery(query);
    queryConfig.setUseLegacySql(false);

    // Set the table to put results into.
    queryConfig.setDestinationTable(tableRef);

    LOG.debug("-> " + queryConfig.getCreateDisposition());
    queryConfig.setCreateDisposition("CREATE_IF_NEEDED");

    // Require table to be empty.
    queryConfig.setWriteDisposition("WRITE_EMPTY");

    JobConfiguration config = new JobConfiguration();
    config.setQuery(queryConfig);

    JobReference jobReference =
      bigQueryHelper.createJobReference(projectId, "querybasedexport", null);

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Run the job.
    Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);

    // Create anonymous Progressable object
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        // TODO(user): ensure task doesn't time out
      }
    };

    // Poll until job is complete.
    BigQueryUtils.waitForJobCompletion(
      bigQueryHelper.getRawBigquery(), projectId, jobReference, progressable);
  }

  @Override
  public void cleanupExport() throws IOException {
    if (deleteIntermediateTable) {
      LOG.info(
        "Deleting input intermediate table: {}:{}.{}",
        tableToExport.getProjectId(),
        tableToExport.getDatasetId(),
        tableToExport.getTableId());

      Bigquery.Tables tables = bigQueryHelper.getRawBigquery().tables();
      Bigquery.Tables.Delete delete = tables.delete(
        tableToExport.getProjectId(), tableToExport.getDatasetId(), tableToExport.getTableId());
      delete.execute();
    }

    delegate.cleanupExport();
  }
}
