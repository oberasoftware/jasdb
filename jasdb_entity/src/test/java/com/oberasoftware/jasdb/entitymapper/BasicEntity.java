package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;

/**
 * @author Renze de Vries
 */
@JasDBEntity(bagName = "MyBAG")
public class BasicEntity {

    private String someField;
    private long anotherNumberField;
    private String justSomeTextField;

    public BasicEntity(String someField, long anotherNumberField, String justSomeTextField) {
        this.someField = someField;
        this.anotherNumberField = anotherNumberField;
        this.justSomeTextField = justSomeTextField;
    }

    public BasicEntity() {
    }

    @JasDBProperty
    public String getSomeField() {
        return someField;
    }

    public void setSomeField(String someField) {
        this.someField = someField;
    }

    @JasDBProperty
    public long getAnotherNumberField() {
        return anotherNumberField;
    }

    public void setAnotherNumberField(long anotherNumberField) {
        this.anotherNumberField = anotherNumberField;
    }

    @JasDBProperty(name = "differentNameThanField")
    public String getJustSomeTextField() {
        return justSomeTextField;
    }

    public void setJustSomeTextField(String justSomeTextField) {
        this.justSomeTextField = justSomeTextField;
    }

    @Override
    public String toString() {
        return "BasicEntity{" +
                "someField='" + someField + '\'' +
                ", anotherNumberField=" + anotherNumberField +
                ", justSomeTextField='" + justSomeTextField + '\'' +
                '}';
    }
}
