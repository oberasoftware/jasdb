package nl.renarj.jasdb.rest.model;

/**
 * @author: renarj
 * Date: 3-6-12
 * Time: 16:42
 */
public class IndexEntry implements RestEntity {
    private String name;
    private String keyHeader;
    private String valueHeader;
    private boolean uniqueConstraint;
    private int indexType;
    private long memorySize;

    public IndexEntry() {

    }

    public IndexEntry(String name, String keyHeader, String valueHeader, boolean uniqueConstraint, int indexType) {
        this.name = name;
        this.keyHeader = keyHeader;
        this.valueHeader = valueHeader;
        this.uniqueConstraint = uniqueConstraint;
        this.indexType = indexType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKeyHeader() {
        return keyHeader;
    }

    public void setKeyHeader(String keyHeader) {
        this.keyHeader = keyHeader;
    }

    public String getValueHeader() {
        return valueHeader;
    }

    public void setValueHeader(String valueHeader) {
        this.valueHeader = valueHeader;
    }

    public boolean isUniqueConstraint() {
        return uniqueConstraint;
    }

    public void setUniqueConstraint(boolean uniqueConstraint) {
        this.uniqueConstraint = uniqueConstraint;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public int getIndexType() {
        return indexType;
    }

    public void setIndexType(int indexType) {
        this.indexType = indexType;
    }
}
