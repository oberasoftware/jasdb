/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.properties;

import com.oberasoftware.jasdb.api.session.Value;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 12:34 PM
 */
public class IntegerValue implements Value {
    private Integer value;

    public IntegerValue(Integer value) {
        this.value = value;
    }
    
    @Override
    public Object getValue() {
        return value;
    }
    
    public Integer toInteger() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        IntegerValue that = (IntegerValue) o;

        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
