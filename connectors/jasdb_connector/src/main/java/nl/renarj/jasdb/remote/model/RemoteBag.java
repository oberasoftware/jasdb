package nl.renarj.jasdb.remote.model;

import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class RemoteBag implements Bag {
    private long size;
    private long diskSize;
    private Bag bag;

    public RemoteBag(Bag bag, long size, long diskSize) {
        this.bag = bag;
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
        return bag.getName();
    }

    @Override
    public String getInstanceId() {
        return bag.getInstanceId();
    }

    @Override
    public List<IndexDefinition> getIndexDefinitions() {
        return bag.getIndexDefinitions();
    }
}
