package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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


    TableRowsGcsStream downloadAsTableRowsStream() {
        try {
            Storage storage = AuthorizationLogic.createAuthorizedStorageClient(authContext.accountId, authContext.p12File);
            // list
            List<Storage.Objects.Get> getObjectList = new ArrayList<Storage.Objects.Get>();
            Storage.Objects.List list = storage.objects().list(bucket).setPrefix(path);
			Objects objects = list.execute();
			List<StorageObject> items = objects.getItems();
			if (items != null) {
				for (StorageObject o : items) {
					Storage.Objects.Get get = storage.objects().get(bucket, o.getName());
					getObjectList.add(get);
				}
			}
            // Fetch from GCS
            return new TableRowsGcsStream(getObjectList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
