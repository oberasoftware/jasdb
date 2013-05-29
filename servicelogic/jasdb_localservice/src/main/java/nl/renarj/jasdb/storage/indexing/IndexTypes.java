/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.indexing;

public enum IndexTypes {
	INVALID(-1, "invalid"),
	BTREE(2, "btree"),
	INVERTED(1, "inverted");
	
	private int type;
	private String name;
	
	IndexTypes(int type, String name) {
		this.type = type;
		this.name = name;
	}
	
	public int getType() {
		return this.type;
	}
	
	public String getName() {
		return name;
	}

	public static IndexTypes getTypeFor(int type) {
		for(IndexTypes indexType : values()) {
			if(indexType.type == type) {
				return indexType;
			}
		}
		return INVALID;
	}
}
