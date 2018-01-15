package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.model.NodeInformation;

/**
 * @author renarj
 */
public interface EngineManager {
    NodeInformation startEngine() throws JasDBException;

    void stopEngine() throws JasDBException;

    String getEngineVersion();

    NodeInformation getNodeInformation();

    MetadataStore getMetadataStore();
}
