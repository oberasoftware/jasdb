package nl.renarj.jasdb.api.engine;

import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.NodeInformation;

/**
 * @author renarj
 */
public interface EngineManager {
    NodeInformation startEngine() throws JasDBException;

    void stopEngine() throws JasDBException;

    String getEngineVersion();

    NodeInformation getNodeInformation();
}
