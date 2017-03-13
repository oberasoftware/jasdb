/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.properties;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.Value;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 1:43 PM
 */
public class EntityValue implements Value {
    private Entity entity;
    
    public EntityValue(Entity entity) {
        this.entity = entity;
    }
    
    public Entity toEntity() {
        return entity;
    }

    @Override
    public Entity getValue() {
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
