/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.service.local;

import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.*;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.index.CompositeIndexField;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.session.EntityBag;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.List;

public class EntityBagImpl implements EntityBag {
	private static final Logger LOG = LoggerFactory.getLogger(EntityBagImpl.class);
	
	private String name;
	
	private String instanceId;
	private StorageService storageService;

    private UserSession userSession;

    protected EntityBagImpl(String instanceId, String name, StorageService storageService) {
        this.instanceId = instanceId;
        this.name = name;
        this.storageService = storageService;
    }

    public EntityBagImpl(String instanceId, String name, UserSession userSession) throws JasDBStorageException {
        this(instanceId, name);
        this.userSession = userSession;
    }
	
	public EntityBagImpl(String instanceId, String name) throws JasDBStorageException {
		this.name = name;
        this.instanceId = instanceId;
		
		try {
            ApplicationContext applicationContext = ApplicationContextProvider.getApplicationContext();
            if(applicationContext.getBean(DBInstanceFactory.class).hasInstance(instanceId)) {
                StorageServiceFactory serviceFactory = applicationContext.getBean(StorageServiceFactory.class);
                this.storageService = serviceFactory.getOrCreateStorageService(instanceId, name);
            } else {
                throw new JasDBStorageException("Unable to load instance, does not exist");
            }
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
	public Entity addEntity(Entity entity) throws JasDBStorageException {
		this.storageService.insertEntity(getContext(), entity);
		return entity;
	}
	
	@Override
	public Entity updateEntity(Entity entity) throws JasDBStorageException {
        this.storageService.updateEntity(getContext(), entity);
        return entity;
	}

	@Override
	public Entity persist(Entity entity) throws JasDBStorageException {
		this.storageService.persistEntity(getContext(), entity);
		return entity;
	}

	@Override
    public void removeEntity(Entity entity) throws JasDBStorageException {
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
		
		return new QueryExecutorImpl(getContext(), instanceId, name, queryBuilder);
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
		
		return new QueryExecutorImpl(getContext(), instanceId, name, queryBuilder);
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
		
		return new QueryExecutorImpl(getContext(), instanceId, name, queryBuilder);
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
	public Entity getEntity(String entityId) throws JasDBStorageException {
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
