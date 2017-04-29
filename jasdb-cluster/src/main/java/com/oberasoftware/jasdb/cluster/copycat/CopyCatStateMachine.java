package com.oberasoftware.jasdb.cluster.copycat;

import com.oberasoftware.jasdb.cluster.copycat.lock.LockStateMachine;
import com.oberasoftware.jasdb.cluster.copycat.lock.LockCommand;
import com.oberasoftware.jasdb.cluster.copycat.lock.UnlockCommand;
import com.oberasoftware.jasdb.cluster.copycat.map.*;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class CopyCatStateMachine extends StateMachine implements Snapshottable {
    private static final Logger LOG = LoggerFactory.getLogger(CopyCatStateMachine.class);

    private MapStateMachine mapStateMachine = new MapStateMachine();
    private LockStateMachine lock = new LockStateMachine();

    public void mapPut(Commit<PutCommand> command) {
        try {
            mapStateMachine.put(command.operation());
        } finally {
            command.close();
        }
    }

    public Object mapGet(Commit<GetCommand> command) {
        try {
            return mapStateMachine.get(command.operation());
        } finally {
            command.close();
        }
    }

    public Collection<Object> mapValues(Commit<GetValuesCommand> command) {
        try {
            return mapStateMachine.get(command.operation());
        } finally {
            command.close();
        }
    }

    public void mapRemove(Commit<RemoveCommand> command) {
        try {
            mapStateMachine.remove(command.operation());
        } finally {
            command.close();
        }
    }

    public Integer mapSize(Commit<GetSizeCommand> command) {
        return mapStateMachine.size(command);
    }

    public void lock(Commit<LockCommand> command) {
        lock.lock(command);
    }

    public void unlock(Commit<UnlockCommand> command) {
        lock.unlock(command);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeObject(mapStateMachine);
    }

    @Override
    public void install(SnapshotReader reader) {
        mapStateMachine = reader.readObject();
    }
}
