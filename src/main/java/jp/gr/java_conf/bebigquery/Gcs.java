package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.storage.Storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 *
 */
public class Gcs {
    private AuthorizationContext authContext;
    private String bucket;
    private String path;

    public Gcs(AuthorizationContext authContext, String bucket, String path) {
        this.authContext = authContext;
        this.bucket = bucket;
        this.path = path;
    }

    public Iterable<TableRow> asIterable() {
        return downloadAsTableRowsStream();
    }

    public Iterator<TableRow> asIterator() {
        return downloadAsTableRowsStream();
    }

    TableRowsStream downloadAsTableRowsStream() {
        try {
            // Fetch from GCS
            Storage storage = AuthorizationUtil.createAuthorizedStorageClient(authContext.accountId, authContext.p12File);
            Storage.Objects.Get get = storage.objects().get(bucket, path);
            InputStream is = get.executeMediaAsInputStream();
            return new TableRowsStream(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
