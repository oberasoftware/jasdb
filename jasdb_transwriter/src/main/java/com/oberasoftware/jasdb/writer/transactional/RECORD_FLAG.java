/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.writer.transactional;

public enum RECORD_FLAG {
	ACTIVE(0),
	DELETED(1),
	EMPTY(2),
    UPDATED(3);
	
	private int flag;
	
	RECORD_FLAG(int flag) {
		this.flag = flag;
	}
	
	public int getFlag() {
		return this.flag;
	}
	
	static RECORD_FLAG getRecordFlag(int flag) {
		for(RECORD_FLAG recordFlag : values()) {
			if(recordFlag.getFlag() == flag) {
				return recordFlag;
			}
		}
		
		return ACTIVE;
	}
}