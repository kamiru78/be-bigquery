package jp.gr.java_conf.bebigquery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BeBigQuery {
    private static Logger logger = LoggerFactory.getLogger(BeBigQuery.class);
    private AuthorizationContext authContext = new AuthorizationContext();

    public BeBigQuery(String projectId, String accountId, File p12File) {
        authContext.projectId = projectId;
        authContext.accountId = accountId;
        authContext.p12File = p12File;
    }

	/**
     * Creates a Query object.
     * @param query
     * @return
     */
    public Query query(String query) {
        return new Query(authContext, query);
    }

}

