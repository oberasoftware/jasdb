package com.oberasoftware.jasdb.cluster.copycat.lock;

import io.atomix.copycat.Command;

/**
 * @author renarj
 */
public class LockCommand implements Command<Void> {
    private String name;

    public LockCommand(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
