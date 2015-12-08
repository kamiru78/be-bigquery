package org.burningbigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 */
public class TableRowResult implements Iterator<TableRow>, Iterable<TableRow> {
    private static Logger logger = LoggerFactory.getLogger(TableRowResult.class);
    private String projectId;
    private Bigquery bigquery;
    private Job completedJob;
    private Iterator<TableRow> part;
    private GetQueryResultsResponse queryResult;
    private String pageToken = null;

    public TableRowResult(String projectId, Bigquery bigquery, Job completedJob) {
        this.projectId = projectId;
        this.bigquery = bigquery;
        this.completedJob = completedJob;
    }

    @Override
    public Iterator<TableRow> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (part == null) {
            fetch();
        }
        if (part.hasNext()) {
            return part.hasNext();
        } else if (queryResult.getPageToken() != null) {
            fetch();
            return part.hasNext();
        } else {
            return false;
        }
    }

    @Override
    public TableRow next() {
        if (!hasNext()) {
            return null;
        }
        return part.next();
    }

    private void fetch() {
        logger.debug("fetching...");
        try {
            logger.debug("execute");
            queryResult = bigquery
                    .jobs()
                    .getQueryResults(projectId,
                            completedJob.getJobReference().getJobId()).setPageToken(pageToken).execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (queryResult == null || queryResult.getRows() == null) {
            // 結果が空の場合
            part = Collections.emptyIterator();
        } else {
            part = queryResult.getRows().iterator();
            pageToken = queryResult.getPageToken();
        }
    }
}