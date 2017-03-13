/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.keys;

import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;

import java.io.UnsupportedEncodingException;

public final class KeyUtil {
    public static final String DEFAULT_ENCODING = "UTF8";
	public static final String RECORD_POINTER = "RECORD_POINTER";

    private KeyUtil() {}
	
    public static UUIDKey getDocumentKey(KeyNameMapper keyNameMapper, Key key) throws JasDBStorageException {
        Key documentKey = key.getKey(keyNameMapper, "__ID");
        if(documentKey != null) {
            return (UUIDKey) documentKey;
        } else {
            throw new JasDBStorageException("No DocumentId present in key value");
        }
    }

    public static boolean isAnyDataPresent(IndexableItem sEntity, Index index) {
        for(String indexField : index.getKeyInfo().getKeyFields()) {
            if(sEntity.hasValue(indexField)) {
                return true;
            }
        }
        return false;
    }

	
    public static byte[] getUnicodeBytes(String data) {
        try {
            return data.getBytes(DEFAULT_ENCODING);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeJasDBException("Unable to retrieve unicode key, character set invalid", e);
        }
    }

    public static String getUnicodeString(byte[] data) {
        try {
            return new String(data, DEFAULT_ENCODING).trim();
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeJasDBException("Unable to load unicode key, character set invalid", e);
        }
    }
}
