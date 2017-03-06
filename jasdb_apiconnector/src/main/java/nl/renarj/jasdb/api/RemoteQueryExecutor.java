/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api;

import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.remote.EntityConnector;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import com.oberasoftware.jasdb.engine.query.BuilderTransformer;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;

import java.util.List;

/**
 * User: renarj
 * Date: 4/25/12
 * Time: 9:55 PM
 */
public class RemoteQueryExecutor implements QueryExecutor {
    private String bag;
    private String instance;
    private NodeInformation nodeInformation;

    private BlockOperation blockOperation;
    private List<SortParameter> sortParameters;
    private SearchLimit limit = new SearchLimit();
    private RemotingContext context;

    protected RemoteQueryExecutor(String instance, RemotingContext context, String bag, NodeInformation nodeInformation, QueryBuilder parentBuilder) {
        this.context = context;
        this.bag = bag;
        this.instance = instance;
        this.nodeInformation = nodeInformation;

        this.blockOperation = BuilderTransformer.transformBuilder(parentBuilder);
        this.sortParameters = parentBuilder.getSortParams();
    }

    @Override
    public QueryExecutor limit(int limit) {
        this.limit = new SearchLimit(limit);
        return this;
    }

    @Override
    public QueryExecutor paging(int start, int max) {
        this.limit = new SearchLimit(start, max);
        return this;
    }

    @Override
    public QueryResult execute() throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        return connector.find(context, instance, bag, blockOperation, limit, sortParameters);
    }
}
