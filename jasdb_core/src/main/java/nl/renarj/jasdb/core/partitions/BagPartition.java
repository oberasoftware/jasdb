package nl.renarj.jasdb.core.partitions;

import nl.renarj.jasdb.core.exceptions.MetadataParseException;

public class BagPartition {
    private String status;
    private String partitionId;
	private String partitionStrategy;
	private String partitionType;
	private String start;
	private String end;
    private long size;
	
	public BagPartition(String partitionId, String strategy, String type, String status, String start, String end, long size) {
        this.partitionId  = partitionId;
		this.partitionStrategy = strategy;
		this.partitionType = type;
        this.status = status;
		this.start = start;
		this.end = end;
        this.size = size;
	}

    public String getPartitionId() {
        return partitionId;
    }

    public String getPartitionStrategy() {
		return partitionStrategy;
	}

	public String getPartitionType() {
		return partitionType;
	}

    public String getStatus() {
        return status;
    }

    public String getStart() {
		return start;
	}

	public String getEnd() {
		return end;
	}

    public long getSize() {
        return size;
    }
    
    public void incrementSize() {
        size++;
    }

    public void setStatus(String state) {
        this.status = state;
    }

    public static BagPartition fromHeader(String header) throws MetadataParseException {
        String[] definitionSections = header.split(";");
        if(definitionSections.length == 7) {
            String partitionId = definitionSections[0];
            String strategy = definitionSections[1];
            String type = definitionSections[2];
            String status = definitionSections[3];
            String start = definitionSections[4];
            String end = definitionSections[5];
            long size = Long.parseLong(definitionSections[6]);

            return new BagPartition(partitionId, strategy, type, status, start, end, size);
        } else {
            throw new MetadataParseException("Unable to parse partition metadata section from header: " + header);
        }
    }
    
    public String toHeader() {
        StringBuilder builder = new StringBuilder();
        builder.append(partitionId).append(";");
        builder.append(partitionStrategy).append(";");
        builder.append(partitionType).append(";");
        builder.append(status).append(";");
        builder.append(start).append(";");
        builder.append(end).append(";");
        builder.append(size).append(";");
        
        return builder.toString();
    }

    @Override
    public int hashCode() {
        return toHeader().hashCode();
    }

    @Override
    public String toString() {
        return toHeader();
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof BagPartition) {
            BagPartition other = (BagPartition) o;
            if(other.getEnd().equals(end) && other.getStart().equals(start) &&
                    other.getPartitionStrategy().equals(partitionStrategy) &&
                    other.getPartitionType().equals(partitionType)) {
                return true;
            }
        }
        return false;
    }
}
