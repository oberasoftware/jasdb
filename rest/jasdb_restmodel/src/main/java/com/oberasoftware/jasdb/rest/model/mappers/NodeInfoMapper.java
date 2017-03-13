package com.oberasoftware.jasdb.rest.model.mappers;

import com.oberasoftware.jasdb.api.model.NodeInformation;
import com.oberasoftware.jasdb.api.model.ServiceInformation;
import com.oberasoftware.jasdb.rest.model.Node;
import com.oberasoftware.jasdb.rest.model.NodeServiceInformation;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class NodeInfoMapper {
    public static Node mapTo(NodeInformation nodeInformation) {
        if(nodeInformation != null) {
            Node mappedNode = new Node();
            mappedNode.setGridId(nodeInformation.getGridId());
            mappedNode.setInstanceId(nodeInformation.getInstanceId());

            List<NodeServiceInformation> nodeServices = new ArrayList<>();
            for(ServiceInformation serviceInformation : nodeInformation.getServiceInformationList()) {
                NodeServiceInformation nodeService = new NodeServiceInformation();
                nodeService.setProperties(serviceInformation.getNodeProperties());
                nodeService.setServiceType(serviceInformation.getServiceType());

                nodeServices.add(nodeService);
            }
            mappedNode.setServices(nodeServices);

            return mappedNode;
        } else {
            return null;
        }
    }

}
