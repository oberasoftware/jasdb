package com.oberasoftware.jasdb.api.model;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface Bag {
    String getName();

    String getInstanceId();

    List<IndexDefinition> getIndexDefinitions();
}
