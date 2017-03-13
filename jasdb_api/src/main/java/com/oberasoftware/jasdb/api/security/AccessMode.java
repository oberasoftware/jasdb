package com.oberasoftware.jasdb.api.security;

/**
 * @author Renze de Vries
 */
public enum AccessMode {
    NONE("NONE", 0),
    CONNECT("CONNECT", 1),
    READ("r", 2),
    WRITE("rw", 3),
    UPDATE("rwu", 4),
    DELETE("rwud", 5),
    ADMIN("ALL", 6);


    private String mode;
    private int rank;

    AccessMode(String mode, int rank) {
        this.mode = mode;
        this.rank = rank;
    }

    public String getMode() {
        return mode;
    }

    public int getRank() {
        return rank;
    }

    public static AccessMode fromMode(String mode) {
        for(AccessMode accessMode : values()) {
            if(accessMode.getMode().equals(mode)) {
                return accessMode;
            }
        }
        return NONE;
    }
}
