package nl.renarj.jasdb.localservice.api;

import nl.renarj.jasdb.LocalDBSessionFactory;
import nl.renarj.jasdb.service.SortAndLimitQueryTest;

/**
 * @author Renze de Vries
 */
public class LocalSortAndLimitQueryTest extends SortAndLimitQueryTest {
    public LocalSortAndLimitQueryTest() {
        super(new LocalDBSessionFactory());
    }
}
