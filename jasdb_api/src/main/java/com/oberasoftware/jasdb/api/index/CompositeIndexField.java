package com.oberasoftware.jasdb.api.index;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface CompositeIndexField {
    List<IndexField> getIndexFields();
}
