/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api.properties;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 12:32 PM
 */
public class LongValue implements Value {
    private Long value;
    public LongValue(Long value) {
        this.value = value;
    }
    
    @Override
    public Object getValue() {
        return value;
    }
    
    public Long toLong() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        LongValue longValue = (LongValue) o;

        return value.equals(longValue.value);
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
