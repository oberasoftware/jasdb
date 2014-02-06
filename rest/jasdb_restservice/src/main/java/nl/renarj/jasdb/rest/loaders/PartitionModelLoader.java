/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.mappers.PartitionModelMapper;
import nl.renarj.jasdb.rest.model.Partition;
import nl.renarj.jasdb.rest.model.PartitionCollection;
import nl.renarj.jasdb.rest.model.RestBag;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.service.partitioning.PartitioningManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
@Component
public class PartitionModelLoader extends AbstractModelLoader {
    private static final String SPLIT_OPERATION = "split";
    private static final String CHANGE_STATUS_OPERATION = "changeState";

    private Logger log = LoggerFactory.getLogger(PartitionModelLoader.class);

    @Inject
    private StorageServiceFactory storageServiceFactory;

    @Override
    public String[] getModelNames() {
        return new String[] {"Partitions"};
    }

    @Override
    public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() instanceof RestBag) {
            RestBag bag = (RestBag) previous.getResult();

//            if(input.getCondition() != null) {
//                return handleConditions(bag, input);
//            } else {
//                return handleRelations(bag);
//            }
            throw new RestException("Not implemented");
        } else {
            throw new RestException("Cannot retrieve partitions without a specified bag");
        }
    }

//    private RestEntity handleRelations(RestBag bag) throws RestException {
//        try {
//            StorageServiceFactory serviceFactory = SimpleKernel.getStorageServiceFactory();
//            BagInfoReader bagInfoReader = serviceFactory.getBagInfo(bag.getInstanceId(), bag.getName());
//
//            Set<BagPartition> partitions = bagInfoReader.getPartitions();
//            return PartitionModelMapper.map(partitions, bag);
//        } catch(ConfigurationException e) {
//            throw new RestException("Unable to load partition data: " + e.getMessage());
//        } catch(JasDBStorageException e) {
//            throw new RestException("Unable to load partition data: " + e.getMessage());
//        }
//    }
//
//    private RestEntity handleConditions(RestBag bag, InputElement input) throws RestException {
//        InputCondition inputCondition = input.getCondition();
//        log.debug("Loading partition information based on input condition: {}", inputCondition);
//        if(inputCondition instanceof FieldCondition) {
//            FieldCondition fieldCondition = (FieldCondition) inputCondition;
//
//            try {
//                StorageServiceFactory serviceFactory = SimpleKernel.getStorageServiceFactory();
//                BagInfoReader bagInfoReader = serviceFactory.getBagInfo(bag.getInstanceId(), bag.getName());
//                BagPartition partition = bagInfoReader.getPartitionById(fieldCondition.getValue());
//
//                if(partition != null) {
//                    log.debug("Found a partition with id: {}", partition.getPartitionId());
//                    return PartitionModelMapper.map(partition, bag);
//                } else {
//                    throw new RestException("No Partition was found with name: " + fieldCondition.getValue());
//                }
//            } catch(JasDBStorageException e) {
//                throw new RestException("Unable to load partition metadata", e);
//            } catch(ConfigurationException e) {
//                throw new RestException("Unable to load partition metadata", e);
//            }
//        } else {
//            throw new SyntaxException("No recognized criteria specified, use parameter name=value or simple value");
//        }
//    }

    @Override
    public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        return null;
    }

    @Override
    public RestEntity doOperation(InputElement input) throws RestException {
        String operation = input.getElementName();
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() != null) {
            RestEntity entity = previous.getResult();
            if(entity instanceof Partition) {
                Partition partition = (Partition) entity;

                try {
                    StorageService storageService = storageServiceFactory.getOrCreateStorageService(partition.getInstance(), partition.getBag());

                    if(operation.equals(SPLIT_OPERATION)) {
                        List<BagPartition> splittedPartitions = storageService.getPartitionManager().splitPartition(PartitionModelMapper.map(partition));

                        return PartitionModelMapper.map(splittedPartitions, partition.getInstance(), partition.getBag());
                    } else if(operation.equals(CHANGE_STATUS_OPERATION) && input.getCondition() != null && input.getCondition() instanceof FieldCondition) {
                        FieldCondition fieldCondition = (FieldCondition) input.getCondition();
                        String targetState = fieldCondition.getValue();
                        PartitioningManager partitioningManager = storageService.getPartitionManager();
                        boolean changeSuccess = partitioningManager.changePartitionStatus(partition.getPartitionId(), targetState);
                        if(changeSuccess) {
                            List<Partition> partitions = new ArrayList<>();
                            Partition mappedPartition = PartitionModelMapper.map(partitioningManager.getLocalPartition(partition.getPartitionId()), partition.getInstance(), partition.getBag());
                            partitions.add(mappedPartition);

                            return new PartitionCollection(partitions);
                        } else {
                            throw new RestException("Unable to modify partition status of: " + partition.getPartitionId());
                        }
                    }
                } catch(JasDBStorageException e) {
                    log.error("Unable to do partition operation", e);
                }


            }
        }

        throw new RestException("Unable to complete operation: " + operation + " on type Partitions");
    }

    @Override
    public boolean isOperationSupported(String operation) {
        return SPLIT_OPERATION.equals(operation) || CHANGE_STATUS_OPERATION.equals(operation);
    }
}
