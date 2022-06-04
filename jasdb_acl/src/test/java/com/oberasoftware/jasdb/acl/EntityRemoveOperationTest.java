package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.engine.StorageService;
import org.springframework.aop.framework.Advised;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
