package com.oberasoftware.jasdb.cluster.copycat.map;

import io.atomix.copycat.Command;

public class RemoveCommand implements Command<Object> {
    private String map;
    private Object key;

    public RemoveCommand(String map, Object key) {
        this.map = map;
        this.key = key;
    }

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "RemoveCommand{" +
                "map='" + map + '\'' +
                ", key=" + key +
                '}';
    }
}
