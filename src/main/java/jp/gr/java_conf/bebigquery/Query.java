package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 *
 */
public class Query {
    private static Logger logger = LoggerFactory.getLogger(Query.class);
    private AuthorizationContext authContext = new AuthorizationContext();
    private String query;

    public Query(AuthorizationContext authContext, String query) {
        this.authContext = authContext;
        this.query = query;
    }
    public Iterable<TableRow> asIterable() {
        return executeAndGetResult();
    }

    public Iterator<TableRow> asIterator() {
        return executeAndGetResult();
    }

    TableRowsResult executeAndGetResult() {
        // Create a new BigQuery client
        final Bigquery bigquery = AuthorizationLogic.createAuthorizedBigQueryClient(authContext.accountId, authContext.p12File);
        // Start a Query Job
        JobConfiguration config = QueryLogic.createSimpleJobConfiguration(query);
        Job completedJob = QueryLogic.executeQuery(authContext.projectId, bigquery, config);

        return new TableRowsResult(authContext.projectId, bigquery, completedJob);
    }

    public Iterable<TableRow> asIterableViaGcs(String tempDatasetId, String tempBucket) {
        return asIterableViaGcs(tempDatasetId, "__burning_bigquery", tempBucket, "__burning_bigquery");
    }

    public Iterable<TableRow> asIterableViaGcs(String tempDatasetId, String tempTableId, String tempBucket, String tempGcsPath) {
        return executeAndGetResultViaGcs(tempDatasetId, tempTableId, tempBucket, tempGcsPath);
    }

    public Iterator<TableRow> asIteratorViaGcs(String tempDatasetId, String tempBucket) {
        return asIteratorViaGcs(tempDatasetId, "__burning_bigquery", tempBucket, "__burning_bigquery");
    }

    public Iterator<TableRow> asIteratorViaGcs(String tempDatasetId, String tempTableId, String tempBucket, String tempGcsPath) {
        return executeAndGetResultViaGcs(tempDatasetId, tempTableId, tempBucket, tempGcsPath);
    }

    TableRowsGcsStream executeAndGetResultViaGcs(String tempDatasetId, String tempTableId, String tempBucket, String tempGcsPath) {
        // Execute query and export to a temp table
        executeQueryAndExportToTable(tempDatasetId, tempTableId);
        // Export to Gcs
        ExportedTable tempTable = new ExportedTable(authContext, tempDatasetId, tempTableId);
        tempTable.exportToGcs(tempBucket, tempGcsPath);
        // Fetch from GCS
        Gcs gcs = new Gcs(authContext, tempBucket, tempGcsPath);
        return gcs.downloadAsTableRowsStream();
    }

    public void exportToTable(String datasetId, String tableId) {
        // Execute query and export to a temp table
        executeQueryAndExportToTable(datasetId, tableId);
    }

    public void exportToGcs(String tempDatasetId, String tempTableId, String bucket, String gcsPath) {
        // Execute query and export to a temp table
        executeQueryAndExportToTable(tempDatasetId, tempTableId);
        // Export to Gcs
        ExportedTable tempTable = new ExportedTable(authContext, tempDatasetId, tempTableId);
        tempTable.exportToGcs(bucket, gcsPath);
    }

    private void executeQueryAndExportToTable(String tempDatasetId, String tempTableId) {
        final Bigquery bigquery = AuthorizationLogic.createAuthorizedBigQueryClient(authContext.accountId, authContext.p12File);
        JobConfiguration config = QueryLogic.createExportToTableJobConfiguration(authContext.projectId, query, tempDatasetId, tempTableId);
        QueryLogic.executeQuery(authContext.projectId, bigquery, config);
    }

}
