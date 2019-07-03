package io.cdap.plugin.gcp.bigquery.source;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.io.bigquery.AbstractBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.AvroRecordReader;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.io.bigquery.Export;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.io.bigquery.NoopFederatedExportToCloudStorage;
import com.google.cloud.hadoop.io.bigquery.ShardedExportToCloudStorage;
import com.google.cloud.hadoop.io.bigquery.UnshardedExportToCloudStorage;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

/**
 * BigQuery input format, splits query from the configuration into list of queries
 * in order to create input splits.
 */
public class CdapBigQueryInputFormat extends AbstractBigQueryInputFormat<LongWritable, GenericData.Record> {
  private static final Logger LOG = LoggerFactory.getLogger(CdapBigQueryInputFormat.class);

  private InputFormat<LongWritable, Text> delegateInputFormat;

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.AVRO;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    final Configuration configuration = context.getConfiguration();
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      LOG.error("Failed to create BigQuery client", gse);
      throw new IOException("Failed to create BigQuery client", gse);
    }
    String exportPath =
      BigQueryConfiguration.getTemporaryPathRoot(configuration, context.getJobID());
    configuration.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, exportPath);
    Export export = constructExport(
      configuration,
      getExportFileFormat(),
      exportPath,
      bigQueryHelper,
      delegateInputFormat);
    export.prepare();

    try {
      export.beginExport();
      export.waitForUsableMapReduceInput();
    } catch (IOException | InterruptedException ie) {
      LOG.error("Error while exporting", ie);
      throw new IOException("Error while exporting", ie);
    }

    List<InputSplit> splits = export.getSplits(context);

    if (LOG.isDebugEnabled()) {
      try {
        // Stringifying a really big list of splits can be expensive, so we guard with
        // isDebugEnabled().
        LOG.debug("getSplits -> {}", HadoopToStringUtil.toString(splits));
      } catch (InterruptedException e) {
        LOG.debug("getSplits -> {}", "*exception on toString()*");
      }
    }
    return splits;
  }


  @Override
  public RecordReader<LongWritable, GenericData.Record> createDelegateRecordReader(InputSplit split,
                                                                                   Configuration configuration)
    throws IOException, InterruptedException {
    Preconditions.checkState(
      split instanceof FileSplit, "AvroBigQueryInputFormat requires FileSplit input splits");
    return new AvroRecordReader();
  }

  private static Export constructExport(
    Configuration configuration, ExportFileFormat format, String exportPath,
    BigQueryHelper bigQueryHelper, InputFormat delegateInputFormat)
    throws IOException {
    LOG.debug("constructExport() with export path {}", exportPath);

    // Extract relevant configuration settings.
    Map<String, String> mandatoryConfig = ConfigurationUtil.getMandatoryConfig(
      configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
    String query = configuration.get(BigQueryConfiguration.INPUT_QUERY_KEY);
    String jobProjectId = mandatoryConfig.get(BigQueryConfiguration.PROJECT_ID_KEY);
    String inputProjectId = mandatoryConfig.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
    String datasetId = mandatoryConfig.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
    String tableName = mandatoryConfig.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY);

    TableReference exportTableReference = new TableReference()
      .setDatasetId(datasetId)
      .setProjectId(inputProjectId)
      .setTableId(tableName);

    boolean enableShardedExport = isShardedExportEnabled(configuration);
    boolean deleteTableOnExit = configuration.getBoolean(
      BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY,
      BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_DEFAULT);
    LOG.debug(
      "isShardedExportEnabled = {}, deleteTableOnExit = {}, tableReference = {}, query = {}",
      enableShardedExport,
      deleteTableOnExit,
      BigQueryStrings.toString(exportTableReference),
      query);

    Export export;
    if (Strings.isNullOrEmpty(query)) {
      Table table = bigQueryHelper.getTable(exportTableReference);

      if (EXTERNAL_TABLE_TYPE.equals(table.getType())) {
        if (Strings.isNullOrEmpty(query)) {
          LOG.info("Table is already external, so skipping export");
          // Otherwise getSplits gets confused.
          setEnableShardedExport(configuration, false);
          return new NoopFederatedExportToCloudStorage(
            configuration, format, bigQueryHelper, jobProjectId, table, delegateInputFormat);
        } else {
          LOG.info("Ignoring use of federated data source, because a query was specified.");
        }
      }

      if (enableShardedExport) {
        export = new ShardedExportToCloudStorage(
          configuration,
          exportPath,
          format,
          bigQueryHelper,
          jobProjectId,
          table);
      } else {
        export = new UnshardedExportToCloudStorage(
          configuration,
          exportPath,
          format,
          bigQueryHelper,
          jobProjectId,
          table,
          delegateInputFormat);
      }
    } else {
      export = new CdapBigQueryExport(configuration, exportPath, format, query, jobProjectId, bigQueryHelper,
                                      exportTableReference, deleteTableOnExit, enableShardedExport);
    }

    return export;
  }
}
