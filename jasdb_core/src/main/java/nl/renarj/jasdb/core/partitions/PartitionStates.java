package nl.renarj.jasdb.core.partitions;

/**
 * User: renarj
 * Date: 3/16/12
 * Time: 11:57 AM
 */
public enum PartitionStates {
    OK("ok", true, true, true, true),
    SPLITTING("splitting", true, true, false, false),
    SYNCING("syncing", true, true, false, false),
    READONLY("readonly", false, true, true, true),
    PARTIAL("partial", false, true, false, false),
    ERROR("error", false, false, false, false);
    
    private String state;
    private boolean writeAllowed;
    private boolean readAllowed;
    private boolean partitionModificationAllowed;
    private boolean complete;
    
    PartitionStates(String state, boolean writeAllowed, boolean readAllowed, boolean complete, boolean partitionModificationAllowed) {
        this.state = state;
        this.complete = complete;
        this.writeAllowed = writeAllowed;
        this.readAllowed = readAllowed;
        this.partitionModificationAllowed = partitionModificationAllowed;
    }

    public boolean isComplete() {
        return complete;
    }

    public String getState() {
        return state;
    }

    public boolean isWriteAllowed() {
        return writeAllowed;
    }

    public boolean isReadAllowed() {
        return readAllowed;
    }

    public boolean isPartitionModificationAllowed() {
        return partitionModificationAllowed;
    }
    
    public static PartitionStates fromString(String stateId) {
        for(PartitionStates state : values()) {
            if(state.getState().equals(stateId)) {
                return state;
            }
        }
        return ERROR;
    }
}
