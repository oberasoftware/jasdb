package com.oberasoftware.jasdb.console.model;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * @author Renze de Vries
 */
public class WebEntity {
    @NotNull
    @Size(min=1)
    private String bag;

    @NotNull
    @Size(min=2)
    private String data;

    private String id;

    public String getBag() {
        return bag;
    }

    public void setBag(String bag) {
        this.bag = bag;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
