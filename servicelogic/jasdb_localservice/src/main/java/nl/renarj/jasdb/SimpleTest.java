package nl.renarj.jasdb;

import java.util.HashMap;
import java.util.Map;

/**
 * @author renarj
 */
public class SimpleTest {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SimpleTest.class);

    public void processBindingConfiguration(String context, String bindingConfig)  {
//        logger.trace("processBindingConfiguration({}, {})", item.getName(), bindingConfig);
//        super.processBindingConfiguration(context, item, bindingConfig);
        String[] segments = bindingConfig.split(":");

        if (segments.length < 1 || segments.length > 3)
            LOG.error("");
//            throw new BindingConfigParseException("invalid number of segments in binding: " + bindingConfig);

        int nodeId;
        try{
            nodeId = Integer.parseInt(segments[0]);
        } catch (Exception e){
//            throw new BindingConfigParseException(segments[1] + " is not a valid node id.");
            LOG.error("", e);
        }

        int endpoint = 1;
        Integer refreshInterval = null;
        Map<String, String> arguments = new HashMap<String, String>();

        for (int i = 1; i < segments.length; i++) {
            try {
                if (segments[i].contains("=")) {
                    for (String keyValuePairString : segments[i].split(",")) {
                        String[] pair = keyValuePairString.split("=");
                        String key = pair[0].trim().toLowerCase();

                        if (key.equals("refresh_interval"))
                            refreshInterval = Integer.parseInt(pair[1].trim());
                        else
                            arguments.put(key, pair[1].trim().toLowerCase());
                    }
                } else {
                    endpoint = Integer.parseInt(segments[i]);
                }
            } catch (Exception e){
//                throw new BindingConfigParseException(segments[i] + " is not a valid argument.");
            }
        }

//        ZWaveBindingConfig config = new ZWaveBindingConfig(nodeId, endpoint, refreshInterval, arguments);
//        addBindingConfig(item, config);
//        items.put(item.getName(), item);
    }


    public static void main(String[] args) {
        new SimpleTest().processBindingConfiguration(null, "4:sensor_multilevel");
    }
}
