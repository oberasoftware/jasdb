package nl.renarj.jasdb.rest.model;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class InstanceCollection implements RestEntity {
    private List<InstanceRest> instances;

    public InstanceCollection(List<InstanceRest> instances) {
        this.instances = instances;
    }

    public InstanceCollection() {

    }

    public List<InstanceRest> getInstances() {
        return instances;
    }

    public void setInstances(List<InstanceRest> instances) {
        this.instances = instances;
    }
}
