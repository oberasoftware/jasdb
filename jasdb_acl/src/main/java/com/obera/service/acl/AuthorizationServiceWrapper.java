package com.obera.service.acl;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.metadata.Constants;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Renze de Vries
 */
@Aspect
@Component
public class AuthorizationServiceWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationServiceWrapper.class);

    @Autowired
    private UserManager userManager;

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.insertEntity(..)) && args(context, entity) && target(storageService)")
    public void insertEntity(ProceedingJoinPoint jp, RequestContext context, SimpleEntity entity, StorageService storageService) throws Throwable {
        LOG.debug("Insert aspect invoked with context: {}", context);

        userManager.authorize(context.getUserSession(), getObjectName(storageService), AccessMode.WRITE);

        LOG.debug("Authorization done on insert of: {}, proceeding for context: {}", entity, context);

        jp.proceed();
    }

    private String getObjectName(StorageService storageService) {
        return Constants.OBJECT_SEPARATOR + storageService.getInstanceId() + "/bags/" + storageService.getBagName();
    }

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.removeEntity(..)) && args(context, entity) && target(storageService)")
    public void removeEntity(ProceedingJoinPoint jp, RequestContext context, SimpleEntity entity, StorageService storageService) throws Throwable {
        LOG.debug("Remove aspect invoked with context: {}", context);

        userManager.authorize(context.getUserSession(), getObjectName(storageService), AccessMode.DELETE);

        LOG.debug("Authorization done on remove of: {}, proceeding for context: {}", entity, context);

        jp.proceed();
    }

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.removeEntity(..)) && args(context, internalId) && target(storageService)")
    public void removeEntity(ProceedingJoinPoint jp, RequestContext context, String internalId, StorageService storageService) throws Throwable {
        LOG.debug("Remove aspect invoked with context: {}", context);

        userManager.authorize(context.getUserSession(), getObjectName(storageService), AccessMode.DELETE);

        LOG.debug("Authorization done on remove of: {}, proceeding for context: {}", internalId, context);

        jp.proceed();
    }

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.updateEntity(..)) && args(context, entity) && target(storageService)")
    public void updateEntity(ProceedingJoinPoint jp, RequestContext context, SimpleEntity entity, StorageService storageService) throws Throwable {
        LOG.debug("Update aspect invoked with context: {}", context);

        userManager.authorize(context.getUserSession(), getObjectName(storageService), AccessMode.UPDATE);

        LOG.debug("Authorization done on update of: {}, proceeding for context: {}", entity, context);

        jp.proceed();
    }

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.getEntityById(..)) && args(context, entity) && target(storageService)")
    public void getEntityById(ProceedingJoinPoint jp, RequestContext context, SimpleEntity entity, StorageService storageService) throws Throwable {
        doReadCheck(context, storageService, jp);
    }

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.getEntities(..)) && args(context) && target(storageService)")
    public void getEntities(ProceedingJoinPoint jp, RequestContext context, StorageService storageService) throws Throwable {
        doReadCheck(context, storageService, jp);
    }

    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.getEntities(..)) && args(context, max) && target(storageService)")
    public void getEntities(ProceedingJoinPoint jp, RequestContext context, int max, StorageService storageService) throws Throwable {
        doReadCheck(context, storageService, jp);
    }


    @Around("execution(* nl.renarj.jasdb.service.*StorageService*.removeEntity(..)) && args(context) && target(storageService)")
    public void search(ProceedingJoinPoint jp, RequestContext context, StorageService storageService) throws Throwable {
        doReadCheck(context, storageService, jp);
    }


    private void doReadCheck(RequestContext requestContext, StorageService storageService, ProceedingJoinPoint jp) throws Throwable {
        LOG.debug("Remove aspect invoked with context: {}", requestContext);
        userManager.authorize(requestContext.getUserSession(), getObjectName(storageService), AccessMode.READ);
        LOG.debug("Authorization done on find operation, proceeding for context: {}", requestContext);

        jp.proceed();
    }


//    @Override
//    public QueryResult search(RequestContext context, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws JasDBStorageException {
//        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.READ);
//        return wrappedService.search(context, blockOperation, limit, params);
//    }
}
