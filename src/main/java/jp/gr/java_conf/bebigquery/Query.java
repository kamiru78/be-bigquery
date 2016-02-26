package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Class for representing query
 */
public class Query {
    private static Logger logger = LoggerFactory.getLogger(Query.class);
	private static String TEMP_TABLE_NAME = "__be_bigquery";
	private static String TEMP_GCS_DIR = "__be_bigquery";
    private AuthorizationContext authContext = new AuthorizationContext();
    private String query;

    public Query(AuthorizationContext authContext, String query) {
        this.authContext = authContext;
        this.query = query;
    }

	/**
	 * Execute the query and get result as the Iterable.
     * @return
     */
    public Iterable<TableRow> asIterable() {
        return executeAndGetResult();
    }

	/**
     * Execute the query and get result as the Iterator.
     * @return
     */
    public Iterator<TableRow> asIterator() {
        return executeAndGetResult();
    }

	/**
     * Execute the query, export to a temp table, export to gcs files then get result as the Iterable.
     * @param tempDatasetId
     * @param tempBucket
     * @return
     */
    public Iterable<TableRow> asIterableViaGcs(String tempDatasetId, String tempBucket) {
        return executeAndGetResultViaGcs(tempDatasetId, TEMP_TABLE_NAME, tempBucket, TEMP_GCS_DIR);
    }

	/**
     * Execute the query, export to a temp table, export to gcs files then get result as the Iterator.
     * @param tempDatasetId
     * @param tempBucket
     * @return
     */
    public Iterator<TableRow> asIteratorViaGcs(String tempDatasetId, String tempBucket) {
        return executeAndGetResultViaGcs(tempDatasetId, "__be_bigquery", tempBucket, "__be_bigquery-*");
    }

	/**
     * Execute the query and export to a table.
     * @param datasetId
     * @param tableId
     */
    public void exportToTable(String datasetId, String tableId) {
        // Execute query and export to a temp table
        executeQueryAndExportToTable(datasetId, tableId);
    }

	/**
     * Execute the query, export to a temp table and export to gcs files.
     * @param tempDatasetId
     * @param tempTableId
     * @param bucket
     * @param gcsPath
     */
    public void exportToGcs(String tempDatasetId, String tempTableId, String bucket, String gcsPath) {
        // Execute query and export to a temp table
        executeQueryAndExportToTable(tempDatasetId, tempTableId);
        // Export to Gcs
        ExportedTable tempTable = new ExportedTable(authContext, tempDatasetId, tempTableId);
        tempTable.exportToGcs(bucket, gcsPath, false);	// csvにヘッダーは出力しない
    }

    protected TableRowsResult executeAndGetResult() {
        // Create a new BigQuery client
        final Bigquery bigquery = AuthorizationLogic.createAuthorizedBigQueryClient(authContext.accountId, authContext.p12File);
        // Start a Query Job
        JobConfiguration config = QueryLogic.createSimpleJobConfiguration(query);
        Job completedJob = QueryLogic.executeQuery(authContext.projectId, bigquery, config);

        return new TableRowsResult(authContext.projectId, bigquery, completedJob);
    }

    protected TableRowsGcsStream executeAndGetResultViaGcs(String tempDatasetId, String tempTableId, String tempBucket, String tempGcsBaseDir) {
        // Execute query and export to a temp table
        Job job = executeQueryAndExportToTable(tempDatasetId, tempTableId);
        // Export to Gcs
        ExportedTable tempTable = new ExportedTable(authContext, tempDatasetId, tempTableId);
		String gcsPath = tempGcsBaseDir + "/" + job.getId();
        tempTable.exportToGcs(tempBucket, gcsPath + "/*", true);	// カラム解析のためにヘッダーもcsvへ出力
        // Fetch from GCS
        Gcs gcs = new Gcs(authContext, tempBucket, gcsPath);
        return gcs.downloadAsTableRowsStream();
    }

    protected Job executeQueryAndExportToTable(String tempDatasetId, String tempTableId) {
        final Bigquery bigquery = AuthorizationLogic.createAuthorizedBigQueryClient(authContext.accountId, authContext.p12File);
        JobConfiguration config = QueryLogic.createExportToTableJobConfiguration(authContext.projectId, query, tempDatasetId, tempTableId);
        Job job = QueryLogic.executeQuery(authContext.projectId, bigquery, config);
		return job;
    }

}
