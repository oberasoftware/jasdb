package nl.renarj.jasdb.localservice.api;

import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;
import com.oberasoftware.jasdb.engine.SortAndLimitQueryTest;

/**
 * @author Renze de Vries
 */
public class LocalSortAndLimitQueryTest extends SortAndLimitQueryTest {
    public LocalSortAndLimitQueryTest() {
        super(new LocalDBSessionFactory());
    }
}
