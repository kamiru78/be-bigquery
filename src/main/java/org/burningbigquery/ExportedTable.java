package org.burningbigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.JobConfiguration;

import static org.burningbigquery.AuthorizationUtil.createAuthorizedBigQueryClient;
import static org.burningbigquery.QueryUtil.createExportToGcsJobConfiguration;

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

    public Gcs exportToGcs(String bucket, String gcsPath) {
        // Export the temp table to GCS
        //TODO tmpGcsPathは「/」はじまりだったらNGチェック
        final Bigquery exportBigquery = createAuthorizedBigQueryClient(authContext.accountId, authContext.p12File);
        JobConfiguration exportJobConfig = createExportToGcsJobConfiguration(authContext.projectId, datasetId, tableId, bucket, gcsPath);
        QueryUtil.executeQuery(authContext.projectId, exportBigquery, exportJobConfig);
        return new Gcs(authContext, bucket, gcsPath);
    }
}
