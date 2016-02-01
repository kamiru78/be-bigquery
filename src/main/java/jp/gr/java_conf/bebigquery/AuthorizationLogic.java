package jp.gr.java_conf.bebigquery;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

/**
 * Authorization Logic
 */
public class AuthorizationLogic {
	// API scope
	private static final List<String> BIGQUERY_SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
	private static final List<String> STORAGE_SCOPES = Arrays.asList(
			StorageScopes.DEVSTORAGE_FULL_CONTROL,
			StorageScopes.DEVSTORAGE_READ_ONLY,
			StorageScopes.DEVSTORAGE_READ_WRITE);

	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	/**
	 * Authorizes the installed application to access user's protected data.
	 */
	public static GoogleCredential authorize(String accountId, File p12File, List<String> scopes) {
		try {
			GoogleCredential credentials = new GoogleCredential.Builder().setTransport(TRANSPORT)
					.setJsonFactory(JSON_FACTORY)
					.setServiceAccountId(accountId)
					.setServiceAccountScopes(scopes)
					.setServiceAccountPrivateKeyFromP12File(p12File)
					.build();
			return credentials;
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates an authorized BigQuery client
	 */
	public static Bigquery createAuthorizedBigQueryClient(String accountId, File p12File) {
		Credential credential = authorize(accountId, p12File, BIGQUERY_SCOPES);
		return new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential)
				.setApplicationName(BeBigQuery.class.getSimpleName())
				.build();
	}

	/**
	 * Creates an authorized GoogleCloudStorage client
	 *
	 * @param accountId
	 * @param p12File
	 * @return
	 */
	public static Storage createAuthorizedStorageClient(String accountId, File p12File) {
		Credential credential = authorize(accountId, p12File, STORAGE_SCOPES);
		return new Storage.Builder(TRANSPORT, JSON_FACTORY, credential)
				.setApplicationName(BeBigQuery.class.getSimpleName())
				.build();
	}
}
