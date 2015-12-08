package org.burningbigquery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BurningBigQuery {
    private static Logger logger = LoggerFactory.getLogger(BurningBigQuery.class);
    private AuthorizationContext authContext = new AuthorizationContext();

    public BurningBigQuery(String projectId, String accountId, File p12File) {
        authContext.projectId = projectId;
        authContext.accountId = accountId;
        authContext.p12File = p12File;
    }

    public Query query(String query) {
        return new Query(authContext, query);
    }

}

