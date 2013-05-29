package nl.renarj.jasdb.service.metadata;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Instance;

/**
 * @author Renze de Vries
 */
public class InstanceMeta implements Instance {
    private String instanceId;
    private String path;

    public InstanceMeta(String instanceId, String path) {
        this.instanceId = instanceId;
        this.path = path;
    }

    public static InstanceMeta fromEntity(SimpleEntity entity) {
        return new InstanceMeta(entity.getValue(Constants.INSTANCE).toString(),
                entity.getValue(Constants.INSTANCE_PATH).toString());
    }

    public static SimpleEntity toEntity(Instance instance) {
        SimpleEntity entity = new SimpleEntity();
        entity.addProperty(Constants.META_TYPE, Constants.INSTANCE_TYPE);
        entity.addProperty(Constants.INSTANCE, instance.getInstanceId());
        entity.addProperty(Constants.INSTANCE_PATH, instance.getPath());
        return entity;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstanceMeta that = (InstanceMeta) o;

        if (!instanceId.equals(that.instanceId)) return false;
        if (!path.equals(that.path)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = instanceId.hashCode();
        result = 31 * result + path.hashCode();
        return result;
    }
}
