package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.JobConfiguration;

import static jp.gr.java_conf.bebigquery.AuthorizationLogic.createAuthorizedBigQueryClient;
import static jp.gr.java_conf.bebigquery.QueryLogic.createExportToGcsJobConfiguration;

/**
 *
 */
public class ExportedTable {

    private AuthorizationContext authContext;
    private String datasetId;
    private String tableId;

    public ExportedTable(AuthorizationContext authContext, String datasetId, String tableId) {
        this.authContext = authContext;
        this.datasetId = datasetId;
        this.tableId = tableId;
    }

    public Gcs exportToGcs(String bucket, String gcsPath, boolean isPrintHeader) {
        // Export the temp table to GCS
        //TODO tmpGcsPathは「/」はじまりだったらNGチェック
        final Bigquery exportBigquery = createAuthorizedBigQueryClient(authContext.accountId, authContext.p12File);
        JobConfiguration exportJobConfig = createExportToGcsJobConfiguration(authContext.projectId, datasetId, tableId, bucket, gcsPath, isPrintHeader);
        QueryLogic.executeQuery(authContext.projectId, exportBigquery, exportJobConfig);
        return new Gcs(authContext, bucket, gcsPath);
    }
}
