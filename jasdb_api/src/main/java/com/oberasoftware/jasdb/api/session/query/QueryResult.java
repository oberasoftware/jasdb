/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.session.query;

import com.oberasoftware.jasdb.api.session.Entity;

import java.io.Closeable;
import java.util.Iterator;

public interface QueryResult extends Iterator<Entity>, Iterable<Entity>, Closeable {
	long size();

    boolean isClosed();

    @Override
    void close();
}
