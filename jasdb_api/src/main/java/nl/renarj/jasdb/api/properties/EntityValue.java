/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api.properties;

import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 1:43 PM
 */
public class EntityValue implements Value {
    private SimpleEntity entity;
    
    public EntityValue(EmbeddedEntity entity) {
        this.entity = entity;
    }
    
    public SimpleEntity toEntity() {
        return entity;
    }

    @Override
    public SimpleEntity getValue() {
        return entity;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        EntityValue that = (EntityValue) o;

        return entity.equals(that.entity);
    }

    @Override
    public int hashCode() {
        return entity.hashCode();
    }
    
    @Override
    public String toString() {
        return entity.toString();
    }
}
