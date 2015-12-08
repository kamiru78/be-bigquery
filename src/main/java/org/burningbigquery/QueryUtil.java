package org.burningbigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class QueryUtil {
    private static Logger logger = LoggerFactory.getLogger(QueryUtil.class);
    protected static final String GCS_EXPORT_FORMAT = "CSV";
    protected static final String EXPORT_WRITE_DISPOSITION = "WRITE_TRUNCATE";

    public static Job executeQuery(String projectId, Bigquery bigquery, JobConfiguration config) {
        String query = "";
        if (config.getQuery() != null) {
            query = config.getQuery().getQuery();
        }
        logger.info("Inserting Query Job: " + query);
        try {
            Job job = new Job();
            job.setConfiguration(config);

            Bigquery.Jobs.Insert insert = bigquery.jobs().insert(projectId, job);
            insert.setProjectId(projectId);
            JobReference jobId = insert.execute().getJobReference();

            logger.debug("Job ID of Query Job is: " + jobId.getJobId());

            // Wait for query
            long startTime = System.currentTimeMillis();
            long elapsedTime;
            while (true) {
                Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
                elapsedTime = System.currentTimeMillis() - startTime;
                logger.debug("Job status (" + elapsedTime + ") : " + pollJob.getStatus().getState());
                if (pollJob.getStatus().getState().equals("DONE")) {
                    return pollJob;
                }
                Thread.sleep(5000);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static JobConfiguration createSimpleJobConfiguration(String query) {
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        queryConfig.setQuery(query);
        config.setQuery(queryConfig);
        return config;
    }

    static JobConfiguration createExportToTableJobConfiguration(String projectId, String query, String datasetId, String tableId) {
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        queryConfig.setQuery(query);
        queryConfig.setAllowLargeResults(true);
        queryConfig.setWriteDisposition(EXPORT_WRITE_DISPOSITION);
        TableReference tableReference = new TableReference();
        tableReference.setProjectId(projectId);
        tableReference.setDatasetId(datasetId);
        tableReference.setTableId(tableId);
        queryConfig.setDestinationTable(tableReference);
        config.setQuery(queryConfig);
        return config;
    }

    static JobConfiguration createExportToGcsJobConfiguration(String projectId, String datasetId, String tableId, String tempBucket, String tempGcsPath) {
        JobConfiguration config = new JobConfiguration();
        JobConfigurationExtract extract = new JobConfigurationExtract();
        TableReference tableReference = new TableReference();
        tableReference.setProjectId(projectId);
        tableReference.setDatasetId(datasetId);
        tableReference.setTableId(tableId);
        extract.setSourceTable(tableReference);
        extract.setDestinationUri("gs://" + tempBucket + "/" + tempGcsPath);
        extract.setDestinationFormat(GCS_EXPORT_FORMAT);
        config.setExtract(extract);
        return config;
    }


}
