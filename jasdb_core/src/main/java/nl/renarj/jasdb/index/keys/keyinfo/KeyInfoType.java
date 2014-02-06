/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.keyinfo;

public enum KeyInfoType {
	SINGLE_KEY(1),
	MULTI_KEY(2);
	
	private int keyInfoId;
	
	private KeyInfoType(int keyInfoId) {
		this.keyInfoId = keyInfoId;
	}
	
	public int getKeyInfoId() {
		return this.keyInfoId;
	}
}