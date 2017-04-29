package com.oberasoftware.jasdb.cluster.copycat.map;

import io.atomix.copycat.Query;

public class GetCommand implements Query<Object> {
    private String map;
    private Object key;

    public GetCommand(String map, Object key) {
        this.map = map;
        this.key = key;
    }

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "GetCommand{" +
                "map='" + map + '\'' +
                ", key='" + key + '\'' +
                '}';
    }
}
