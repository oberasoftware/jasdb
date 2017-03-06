package com.oberasoftware.jasdb.engine.search;

import com.oberasoftware.jasdb.engine.query.operators.BlockMerger;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;
import com.oberasoftware.jasdb.engine.query.operators.DistinctCollectionUtil;
import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.engine.IndexManager;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.query.Order;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapperImpl;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.SearchCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QuerySearchOperation {
	private static final Logger LOG = LoggerFactory.getLogger(QuerySearchOperation.class);
	
	private RecordWriter recordWriter;
	private IndexManager indexManager;
	private String bagName;
	
	public QuerySearchOperation(String bagName, IndexManager indexManager, RecordWriter recordWriter) {
		this.recordWriter = recordWriter;
		this.bagName = bagName;
		this.indexManager = indexManager;
	}
	
	public QueryResult search(BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws JasDBStorageException {
		StatRecord record = StatisticsMonitor.createRecord("bag:search:blockHierarchy");
		IndexSearchResultIteratorCollection results = doBlockHierarchy(blockOperation, limit);
		record.stop();

        results = results != null ? results : new IndexSearchResultIteratorImpl(new ArrayList<>(0), new KeyNameMapperImpl());

		if(params != null && !params.isEmpty()) {
			record = StatisticsMonitor.createRecord("bag:search:sort");
			results = doSort(results, params);
			record.stop();
		}
        
        if(limit.getBegin() >= 0 && limit.getMax() > 0) {
            List<Key> k = results.subset(limit.getBegin(), limit.getMax());
            results = new IndexSearchResultIteratorImpl(k, results.getKeyNameMapper());
        }
        results = DistinctCollectionUtil.distinct(results);

        return new QueryResultIteratorImpl(results, limit, this.recordWriter);
	}
	
	private IndexSearchResultIteratorCollection doSort(IndexSearchResultIteratorCollection results, List<SortParameter> params) throws JasDBStorageException {
		Set<String> requiredKeys = new HashSet<>();
		for(SortParameter sortParam : params) {
			String paramField = sortParam.getField();
			if(!results.getKeyNameMapper().isMapped(paramField)) {
				LOG.debug("Sorting field: {} not present in index results following keys present: {}", paramField, results.getKeyNameMapper());
				requiredKeys.add(paramField);
			}
		}
		ensureSortingParams(results, requiredKeys);
		
		for(SortParameter sortParam : params) {
            String field = sortParam.getField();
            if(results.getKeyNameMapper().isMapped(field)) {
                List<Key> sortedKeys = doMergeSort(results.getKeys(), sortParam.getField(), sortParam.getOrder(), results.getKeyNameMapper());

                results = new IndexSearchResultIteratorImpl(sortedKeys, results.getKeyNameMapper());
            }
		}
		
		return results;
	}
	
	private List<Key> doMergeSort(List<Key> results, final String field, final Order order, KeyNameMapper keyNameMapper) throws JasDBStorageException {
		int listSize = results.size();
		if(listSize > 1) {
			int half = listSize / 2;
			List<Key> left = results.subList(0, half);
			left = doMergeSort(left, field, order, keyNameMapper);
			
			List<Key> right = results.subList(half, listSize);
			right = doMergeSort(right, field, order, keyNameMapper);
			
			return merge(left, right, field, order, keyNameMapper);
		} else {
			return results;
		}
	}
	
	private List<Key> merge(List<Key> left, List<Key> right, final String field, final Order order, final KeyNameMapper keyNameMapper) {
		List<Key> results = new ArrayList<>(left.size() + right.size());
		int currentLeft = 0;
		int currentRight = 0;
		
		int leftSize = left.size();
		int rightSize = right.size();
		
		while(currentLeft < leftSize || currentRight < rightSize) {
			if(currentLeft < leftSize && currentRight < rightSize) {
				Key firstLeft = left.get(currentLeft);
				Key firstRight = right.get(currentRight);

				Key leftValue = firstLeft.getKey(keyNameMapper, field);
				Key rightValue = firstRight.getKey(keyNameMapper, field);
				
				if((order == Order.ASCENDING && leftValue.compareTo(rightValue) <= 0) || (order == Order.DESCENDING && leftValue.compareTo(rightValue) >= 0)) {
					results.add(firstLeft);
					currentLeft++;
				} else {
					results.add(firstRight);
					currentRight++;
				}
			} else if(currentLeft < leftSize) {
				results.add(left.get(currentLeft));
				currentLeft++;
			} else if(currentRight < rightSize) {
				results.add(right.get(currentRight));
				currentRight++;
			}
		}
		
		return results;
	}
	
	private IndexSearchResultIterator ensureSortingParams(IndexSearchResultIterator results, Set<String> requiredFields) throws JasDBStorageException {
		if(!requiredFields.isEmpty()) {
            KeyNameMapper keyNameMapper = results.getKeyNameMapper();
			for(Key key : results) {
				UUIDKey documentKey = KeyUtil.getDocumentKey(results.getKeyNameMapper(), key);
				RecordResult recordResult = recordWriter.readRecord(documentKey);
				SimpleEntity entity = SimpleEntity.fromStream(recordResult.getStream());
				
				for(String requiredField : requiredFields) {
					Property property = entity.getProperty(requiredField);
					if(property != null) {
                        Key propertyKey = PropertyKeyMapper.mapToKey(property);
                        keyNameMapper.addMappedField(requiredField);
						key.addKey(keyNameMapper, requiredField, propertyKey);
					}
				}
			}
			
			results.reset();
		}
		
		return results;
	}
	
	private IndexSearchResultIteratorCollection doBlockHierarchy(BlockOperation blockOperation, SearchLimit limit) throws JasDBStorageException {
		StatRecord record = StatisticsMonitor.createRecord("bag:search:blockoperation");
		IndexSearchResultIteratorCollection results = doBlockOperation(blockOperation, limit, blockOperation.getFields(), null);
		record.stop();
		
		record = StatisticsMonitor.createRecord("bag:search:childblockMerge");
		BlockMerger merger = blockOperation.getMerger();
		for(BlockOperation childBlock : blockOperation.getChildBlocks()) {
			IndexSearchResultIteratorCollection childResults = doBlockHierarchy(childBlock, limit);
			if(results != null) {
				results = merger.mergeIterators(results, childResults);
			} else {
				results = childResults;
			}
		}
		record.stop();
		
		return results;
	}
	
	private IndexSearchResultIteratorCollection doBlockOperation(BlockOperation blockOperation, SearchLimit limit,
																 Set<String> fields,
																 IndexSearchResultIteratorCollection currentResults)
			throws JasDBStorageException {
		if(!fields.isEmpty()) {
			Index bestIndexMatch = indexManager.getBestMatchingIndex(bagName, fields);
			
			if(bestIndexMatch != null) {
				Set<String> remainingFields = new HashSet<>(fields);
				remainingFields.removeAll(bestIndexMatch.getKeyInfo().getKeyFields());
				
				IndexSearchResultIteratorCollection results = null;
				BlockMerger merger = blockOperation.getMerger();
				
				Set<SearchCondition> conditions = getSearchConditions(blockOperation, bestIndexMatch);
				for(SearchCondition condition : conditions) {
					StatRecord record = StatisticsMonitor.createRecord("bag:search:indexcondition");
					IndexSearchResultIteratorCollection indexResults = bestIndexMatch.searchIndex(condition, limit);
					record.stop();
					
					if(results != null) {
						StatRecord recordMerge = StatisticsMonitor.createRecord("bag:search:blockoperation:merge");
						results = merger.mergeIterators(results, indexResults);
						recordMerge.stop();
					} else {
						results = indexResults;
					}
				}

				if(!remainingFields.isEmpty()) {
					results = merger.mergeIterators(results, doBlockOperation(blockOperation, limit, remainingFields, results));
				}
				
				return results;
			} else {
				return new TableScanOperation(recordWriter).doTableScan(blockOperation, fields, currentResults);
			}
		} else if(blockOperation.isEmpty()) {
            return new TableScanOperation(recordWriter).doTableScanFindAll();
		} else {
            return null;
        }
	}

	private Set<SearchCondition> getSearchConditions(BlockOperation blockOperation, Index index) {
		KeyInfo keyInfo = index.getKeyInfo();
		return blockOperation.getConditions(keyInfo.getKeyNameMapper(), keyInfo.getKeyFields());
	}
}
