package com.obera.service.acl;

import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.StorageService;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Renze de Vries
 */
public class EntityGetOperationTest extends AbstractAuthorizationTest {
    public EntityGetOperationTest() {
        super(AccessMode.NONE, AccessMode.READ);
    }

    @Override
    protected AuthorizationOperation getOperation() {
        return new AuthorizationOperation() {
            @Override
            public void doOperation(StorageService wrappedService, String user, String password) throws Exception {
                String id = UUID.randomUUID().toString();
                wrappedService.getEntityById(createContext(user, password, "localhost"), id);

                Advised advised = (Advised) wrappedService;
                StorageService mock = (StorageService) advised.getTargetSource().getTarget();
                verify(mock, times(1)).getEntityById(any(RequestContext.class), eq(id));
            }
        };
    }
}
