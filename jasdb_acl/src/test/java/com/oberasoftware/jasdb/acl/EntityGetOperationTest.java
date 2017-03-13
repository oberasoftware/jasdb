package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.core.context.RequestContext;
import org.springframework.aop.framework.Advised;

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
        return (wrappedService, user, password) -> {
            String id = UUID.randomUUID().toString();
            wrappedService.getEntityById(createContext(user, password), id);

            Advised advised = (Advised) wrappedService;
            StorageService mock = (StorageService) advised.getTargetSource().getTarget();
            verify(mock, times(1)).getEntityById(any(RequestContext.class), eq(id));
        };
    }
}
