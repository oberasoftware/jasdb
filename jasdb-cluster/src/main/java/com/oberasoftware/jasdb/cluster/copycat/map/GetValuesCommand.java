package com.oberasoftware.jasdb.cluster.copycat.map;

import io.atomix.copycat.Query;

import java.util.Collection;

public class GetValuesCommand implements Query<Collection<Object>> {
    private String map;

    public GetValuesCommand(String map) {
        this.map = map;
    }

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    @Override
    public String toString() {
        return "GetValuesCommand{" +
                "map='" + map + '\'' +
                '}';
    }
}
