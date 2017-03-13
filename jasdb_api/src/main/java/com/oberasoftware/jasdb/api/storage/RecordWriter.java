/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.storage;


import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * This is the interface that any record writers need to implement. Main goal of any record writer is to persist data
 * streams to a persistent storage medium. Any guarantees about transactionality, disk flushing strategy is up to the implementation
 * of the record writer.
 * 
 * @author Renze de Vries
 *
 */
public interface RecordWriter<T> {

    /**
     * Gets the disk size of the persistence storage
     * @return The disksize in bytes
     * @throws JasDBStorageException If unable to get the disksize
     */
	long getDiskSize() throws JasDBStorageException;

    /**
     * Gets the amount of data streams / records stored
     * @return The amount of datastreams / records stored
     * @throws JasDBStorageException If unable to get the size
     */
	long getSize() throws JasDBStorageException;
	
	/**
	 * This opens the writer for all access, needs to be done before any operations are done on the recordwriter
	 * @throws JasDBStorageException If unable to open the writer
	 */
	void openWriter() throws JasDBStorageException;

	/**
	 * This closes the writer channel and releases all the resources
	 * 
	 * @throws JasDBStorageException If unable to close the writer
	 */
	void closeWriter() throws JasDBStorageException;

    /**
     * Guarantees the changes are flushed to the disk
     * @throws JasDBStorageException If unable to flush changes to the disk
     */
    void flush() throws JasDBStorageException;

	/**
	 * Checks if the writer is already open
	 * @return True if the writer is open and ready for usage, False if not
	 */
	boolean isOpen();
	
	/**
	 * This returns an iterator that can iterate over all persisted records.
	 * 
	 * @return The record iterator which can iterate over all the persisted record.
	 * @throws JasDBStorageException If unable to return an iterator, channel is closed or not accessible
	 */
	RecordIterator readAllRecords() throws JasDBStorageException;
	
	/**
	 * This returns an iterator that can iterate over all persisted records to a defined limit.
	 * 
	 * @return The record iterator which can iterate over all the persisted record to a defined limit.
	 * @throws JasDBStorageException If unable to return an iterator, channel is closed or not accessible
	 */
	RecordIterator readAllRecords(int limit) throws JasDBStorageException;
	
	/**
	 * This reads a specific record at the indicated record position
	 *
     * @param documentId The document key identifier used to identify the document to write to storage
	 * @return The RecordResult if the record could be found
	 * @throws JasDBStorageException In case the record could not be found or read
	 */
	RecordResult readRecord(T documentId) throws JasDBStorageException;

	/**
	 * This writes the given content to the filesystem and returns the record pointer at which the record
	 * can be retrieved on next attempt.
	 *
     * @param documentId The document key identifier used to identify the document to write to storage
     * @param dataStream The datastream to be written to storage
	 * @throws JasDBStorageException If unable to write the record to the storage
	 */
	void writeRecord(T documentId, ClonableDataStream dataStream) throws JasDBStorageException;

	/**
	 * This will command the writer to remove the indicated record from storage.
	 * 
	 * @param documentId The document key identifier used to identify the document to remove
	 * @throws JasDBStorageException If unable to find the indicated record or removing failed.
	 */
	void removeRecord(T documentId) throws JasDBStorageException;

	/**
	 * This updates the record contents stored in storage for a given updated content and the current
	 * record pointer to the record. It will return the same or an updated record pointer pointing to the
	 * updated content on the disk.
	 *
     * @param documentId The document key identifier used to identify the document to write to storage
     * @param dataStream The datastream to be written to storage
     *
	 * @throws JasDBStorageException If unable to update in case no record exists or unable to update in storage.
	 */
	void updateRecord(T documentId, ClonableDataStream dataStream) throws JasDBStorageException;
}