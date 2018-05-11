package com.oberasoftware.jasdb.cluster.copycat.map;

import io.atomix.copycat.Query;

/**
 * @author Renze de Vries
 */
public class GetSizeCommand implements Query<Integer> {
    private String mapName;

    public GetSizeCommand(String mapName) {
        this.mapName = mapName;
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public String toString() {
        return "GetSizeCommand{" +
                "mapName='" + mapName + '\'' +
                '}';
    }
}
