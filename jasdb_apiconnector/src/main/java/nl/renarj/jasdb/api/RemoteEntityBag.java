/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api;

import com.google.common.collect.Lists;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.CompositeQueryField;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryField;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.remote.BagConnector;
import nl.renarj.jasdb.remote.EntityConnector;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public final class RemoteEntityBag implements EntityBag {
    private String instance;
    private Bag meta;
    private NodeInformation nodeInformation;
    private RemotingContext context;

    protected RemoteEntityBag(String instance, RemotingContext context, NodeInformation nodeInformation, Bag bagMeta) {
        this.context = context;
        this.nodeInformation = nodeInformation;
        this.instance = instance;
        this.meta = bagMeta;
    }

    @Override
    public String getName() throws JasDBStorageException {
        return meta.getName();
    }

    @Override
    public long getSize() throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        return connector.getBag(context, instance, meta.getName()).getSize();
    }

    @Override
    public long getDiskSize() throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        return connector.getBag(context, instance, meta.getName()).getDiskSize();
    }

    @Override
    public void flush() throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        connector.flushBag(context, instance, meta.getName());
    }

    @Override
    public SimpleEntity addEntity(SimpleEntity entity) throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        return connector.insertEntity(context, instance, meta.getName(), entity);
    }

    @Override
    public SimpleEntity updateEntity(SimpleEntity entity) throws JasDBStorageException {
        return persist(entity);
    }

    @Override
    public SimpleEntity persist(SimpleEntity entity) throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        return connector.updateEntity(context, instance, meta.getName(), entity);
    }

    @Override
    public void removeEntity(SimpleEntity entity) throws JasDBStorageException {
        removeEntity(entity.getInternalId());
    }

    @Override
    public void removeEntity(String entityId) throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        connector.removeEntity(context, instance, meta.getName(), entityId);
    }

    @Override
    public void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        KeyInfo keyInfo = new KeyInfoImpl(indexField, valueFields);
        connector.createIndex(context, instance, meta.getName(),
                new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), -1), isUnique);
    }

    @Override
    public void ensureIndex(CompositeIndexField queryFields, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        KeyInfo keyInfo = new KeyInfoImpl(queryFields.getIndexFields(), Lists.newArrayList(valueFields));

        connector.createIndex(context, instance, meta.getName(),
                new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), -1), isUnique);
    }

    @Override
    public void removeIndex(String indexKeyName) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        connector.removeIndex(context, instance, meta.getName(), indexKeyName);
    }

    @Override
    public List<String> getIndexNames() throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        List<IndexDefinition> indexDefinitions = connector.getIndexDefinitions(context, instance, meta.getName());
        List<String> indexNames = new ArrayList<>(indexDefinitions.size());
        for(IndexDefinition definition : indexDefinitions) {
            indexNames.add(definition.getIndexName());
        }
        return indexNames;
    }

    @Override
    public QueryExecutor find(QueryField queryField, SortParameter... params) throws JasDBStorageException {
        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.field(queryField);
        setSortParams(queryBuilder, params);

        return new RemoteQueryExecutor(instance, context, meta.getName(), nodeInformation, queryBuilder);
    }


    @Override
    public QueryExecutor find(CompositeQueryField queryFields, SortParameter... params) throws JasDBStorageException {
        QueryBuilder queryBuilder = new QueryBuilder();
        for(QueryField queryField : queryFields.getFields()) {
            queryBuilder.field(queryField);
        }
        setSortParams(queryBuilder, params);

        return new RemoteQueryExecutor(instance, context, meta.getName(), nodeInformation, queryBuilder);
    }

    @Override
    public QueryExecutor find(QueryBuilder queryBuilder) throws JasDBStorageException {
        return new RemoteQueryExecutor(instance, context, meta.getName(), nodeInformation, queryBuilder);
    }

    private void setSortParams(QueryBuilder builder, SortParameter... params) {
        for(SortParameter sortParam: params) {
            builder.sortBy(sortParam);
        }
    }

    @Override
    public QueryResult getEntities() throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        return connector.find(context, instance, meta.getName());
    }

    @Override
    public QueryResult getEntities(int max) throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        return connector.find(context, instance, meta.getName(), max);
    }

    @Override
    public SimpleEntity getEntity(String entityId) throws JasDBStorageException {
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);
        return connector.findById(context, instance, meta.getName(), entityId);
    }
}
