/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.core.utils.StringUtils;

/**
 * @author Renze de Vries
 */
public class RequestContextUtil {
    private RequestContextUtil() {

    }

    public static boolean isClientRequest(String requestContext) {
        if(StringUtils.stringNotEmpty(requestContext)) {
            if(requestContext.equals("grid")) {
                return false;
            }
        }

        return true;
    }
}
