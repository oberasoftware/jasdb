package com.obera.jasdb.web.model;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * @author renarj
 */
public class WebEntity {
    @NotNull
    @Size(min=1)
    private String bag;

    @NotNull
    @Size(min=2)
    private String data;

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
}
