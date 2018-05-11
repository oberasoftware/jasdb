package com.oberasoftware.jasdb.cluster.copycat.lock;

import io.atomix.copycat.Command;

/**
 * @author Renze de Vries
 */
public class UnlockCommand implements Command<Void> {
    private String name;

    public UnlockCommand(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
