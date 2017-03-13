package nl.renarj.jasdb.remote.model;

import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class RemoteBag implements Bag {
    private final String instanceId;
    private final String bagName;
    private final List<IndexDefinition> indexDefinitions;
    private final long size;
    private final long diskSize;

    public RemoteBag(String instanceId, String bagName, List<IndexDefinition> indexDefinitions, long size, long diskSize) {
        this.instanceId = instanceId;
        this.bagName = bagName;
        this.indexDefinitions = indexDefinitions;
        this.size = size;
        this.diskSize = diskSize;
    }

    /**
     * Gets the size of the bag
     * @return The size of the bag
     */
    public long getSize() {
        return this.size;
    }

    /**
     * Gets the disk size of the bag
     * @return The disk size of the bag
     */
    public long getDiskSize() {
        return this.diskSize;
    }

    @Override
    public String getName() {
        return bagName;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public List<IndexDefinition> getIndexDefinitions() {
        return indexDefinitions;
    }
}
