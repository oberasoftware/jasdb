/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.entities;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.CompositeQueryField;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryField;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.storage.query.QueryExecutorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EntityBagImpl implements EntityBag {
	private static final Logger LOG = LoggerFactory.getLogger(EntityBagImpl.class);
	
	private String name;
	
	private DBInstance instance;
	private StorageService storageService;

    private UserSession userSession;

    public EntityBagImpl(String instanceId, String name, UserSession userSession) throws JasDBStorageException {
        this(instanceId, name);
        this.userSession = userSession;
    }
	
	public EntityBagImpl(String instanceId, String name) throws JasDBStorageException {
		this.name = name;
		
		try {
			DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
			if(StringUtils.stringNotEmpty(instanceId)) {
				instance = instanceFactory.getInstance(instanceId);
			} else {
				instance = instanceFactory.getInstance();
			}
			StorageServiceFactory serviceFactory = SimpleKernel.getStorageServiceFactory();
			this.storageService = serviceFactory.getOrCreateStorageService(instanceId, name);
		} catch(ConfigurationException e) {
			throw new JasDBStorageException("Unable to retrieve instance", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.model.EntityBag#getName()
	 */
	@Override
	public String getName() {
		return this.name;
	}
	
	@Override
	public long getSize() throws JasDBStorageException {
		return storageService.getSize();
	}

	@Override
	public long getDiskSize() throws JasDBStorageException {
		return storageService.getDiskSize();
	}

    @Override
    public void flush() throws JasDBStorageException {
        storageService.flush();
    }

    /* (non-Javadoc)
         * @see nl.renarj.pojodb.model.EntityBag#addEntity(nl.renarj.pojodb.model.PojoEntity)
         */
	@Override
	public SimpleEntity addEntity(SimpleEntity entity) throws JasDBStorageException {
		this.storageService.insertEntity(getContext(), entity);
		return entity;
	}
	
	@Override
	public SimpleEntity updateEntity(SimpleEntity entity) throws JasDBStorageException {
        this.storageService.updateEntity(getContext(), entity);
        return entity;
	}

    @Override
    public void removeEntity(SimpleEntity entity) throws JasDBStorageException {
        this.storageService.removeEntity(getContext(), entity);
    }

    @Override
    public void removeEntity(String entityId) throws JasDBStorageException {
        this.storageService.removeEntity(getContext(), entityId);
    }

    /* (non-Javadoc)
          * @see nl.renarj.pojodb.model.EntityBag#find(nl.renarj.pojodb.query.QueryField, nl.renarj.pojodb.query.SortParameter)
          */
	@Override
	public QueryExecutor find(QueryField queryField, SortParameter... params) throws JasDBStorageException {
		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.field(queryField);
		setSortParams(queryBuilder, params);
		
		return new QueryExecutorImpl(getContext(), instance.getInstanceId(), name, queryBuilder);
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.model.EntityBag#find(nl.renarj.pojodb.query.CompositeQueryField, nl.renarj.pojodb.query.SortParameter)
	 */
	@Override
	public QueryExecutor find(CompositeQueryField queryFields, SortParameter... params) throws JasDBStorageException {
		QueryBuilder queryBuilder = new QueryBuilder();
		for(QueryField queryField : queryFields.getFields()) {
			queryBuilder.field(queryField);
		}
		setSortParams(queryBuilder, params);
		
		return new QueryExecutorImpl(getContext(), instance.getInstanceId(), name, queryBuilder);
	}
	
	private void setSortParams(QueryBuilder builder, SortParameter... params) {
		for(SortParameter sortParam: params) {
			builder.sortBy(sortParam);
		}
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.model.EntityBag#find(nl.renarj.pojodb.query.QueryBuilder)
	 */
	@Override
	public QueryExecutor find(QueryBuilder queryBuilder) throws JasDBStorageException {
        LOG.debug("Executing query: {}", queryBuilder);
		
		return new QueryExecutorImpl(getContext(), instance.getInstanceId(), name, queryBuilder);
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.model.EntityBag#getEntities()
	 */
	@Override
	public QueryResult getEntities() throws JasDBStorageException {
		return storageService.getEntities(getContext());
	}

	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.model.EntityBag#getEntities(int)
	 */
	@Override
	public QueryResult getEntities(int max) throws JasDBStorageException {
		return storageService.getEntities(getContext(), max);
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.model.EntityBag#getEntity(java.lang.String)
	 */
	@Override
	public SimpleEntity getEntity(String entityId) throws JasDBStorageException {
		return storageService.getEntityById(getContext(), entityId);
	}
		
	@Override
	public void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        storageService.ensureIndex(indexField, isUnique, valueFields);
	}
	
	@Override
	public void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        storageService.ensureIndex(indexField, isUnique, valueFields);
	}

    @Override
    public void removeIndex(String indexKeyName) throws JasDBStorageException {
        storageService.removeIndex(indexKeyName);
    }

    @Override
    public List<String> getIndexNames() throws JasDBStorageException {
        return storageService.getIndexNames();
    }

    private RequestContext getContext() {
        RequestContext requestContext = new RequestContext(true, true);
        requestContext.setUserSession(userSession);
        return requestContext;
    }
}
