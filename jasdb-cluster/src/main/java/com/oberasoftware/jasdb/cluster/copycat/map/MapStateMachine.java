package com.oberasoftware.jasdb.cluster.copycat.map;

import io.atomix.copycat.server.Commit;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class MapStateMachine implements Serializable {
    private static final Logger LOG = getLogger(MapStateMachine.class);

    private Map<String, Map<Object, Object>> namedMaps = new HashMap<>();

    public void put(PutCommand command) {
        String mapName = command.getMapName();
        namedMaps.putIfAbsent(mapName, new HashMap<>());
        namedMaps.get(mapName).put(command.getKey(), command.getValue());
    }

    public Object get(GetCommand command) {
        if(namedMaps.containsKey(command.getMap())) {
            return namedMaps.get(command.getMap()).get(command.getKey());
        } else {
            return null;
        }
    }

    public Collection<Object> get(GetValuesCommand command) {
        if(namedMaps.containsKey(command.getMap())) {
            return new ArrayList<>(namedMaps.get(command.getMap()).values());
        } else {
            return new ArrayList<>();
        }
    }

    public void remove(RemoveCommand command) {
        if(namedMaps.containsKey(command.getMap())) {
            namedMaps.get(command.getMap()).remove(command.getKey());
        }
    }

    public Integer size(Commit<GetSizeCommand> command) {
        try {
            GetSizeCommand sizeCommand = command.command();
            if(namedMaps.containsKey(sizeCommand.getMapName())) {
                int size = namedMaps.get(sizeCommand.getMapName()).size();
                LOG.info("Returning map size: {}", size);
                return size;
            } else {
                LOG.info("Returning 0");
                return 0;
            }
        } finally {
            command.close();
        }
    }
}
