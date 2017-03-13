package com.oberasoftware.jasdb.rest.service.input;

/**
 * @author Renze de Vries
 *         Date: 15-6-12
 *         Time: 13:11
 */
public class OrderParam {
    public enum DIRECTION {
        ASC,
        DESC
    }

    private String field;
    private DIRECTION sortDirection;

    public OrderParam(String field, DIRECTION sortDirection) {
        this.field = field;
        this.sortDirection = sortDirection;
    }

    public String getField() {
        return field;
    }

    public DIRECTION getSortDirection() {
        return sortDirection;
    }
}
