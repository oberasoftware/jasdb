package com.oberasoftware.jasdb.acl;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.context.RequestContext;
import com.oberasoftware.jasdb.engine.StorageService;
import org.springframework.aop.framework.Advised;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Renze de Vries
 */
public class EntityRemoveOperationTest extends AbstractAuthorizationTest {
    public EntityRemoveOperationTest() {
        super(AccessMode.READ, AccessMode.DELETE);
    }

    @Override
    protected AuthorizationOperation getOperation() {
        return (wrappedService, user, password) -> {
            SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString());
            wrappedService.removeEntity(createContext(user, password), entity);

            Advised advised = (Advised) wrappedService;
            StorageService mock = (StorageService) advised.getTargetSource().getTarget();
            verify(mock, times(1)).removeEntity(any(RequestContext.class), eq(entity));
        };
    }
}
