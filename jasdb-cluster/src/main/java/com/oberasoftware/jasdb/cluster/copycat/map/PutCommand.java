package com.oberasoftware.jasdb.cluster.copycat.map;

import io.atomix.copycat.Command;

public class PutCommand implements Command<Object> {
    private String mapName;
    private Object key;
    private Object value;

    public PutCommand(String mapName, Object key, Object value) {
        this.mapName = mapName;
        this.key = key;
        this.value = value;
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "PutCommand{" +
                "mapName='" + mapName + '\'' +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
