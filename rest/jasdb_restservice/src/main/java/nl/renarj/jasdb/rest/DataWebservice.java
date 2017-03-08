package nl.renarj.jasdb.rest;

import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.engine.EngineManager;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.OrderParameterParsing;
import nl.renarj.jasdb.rest.input.PathParser;
import nl.renarj.jasdb.rest.loaders.ModelLoaderFactory;
import nl.renarj.jasdb.rest.loaders.PathModelLoader;
import nl.renarj.jasdb.rest.loaders.RequestContextUtil;
import nl.renarj.jasdb.rest.mappers.NodeInfoMapper;
import nl.renarj.jasdb.rest.model.ErrorEntity;
import nl.renarj.jasdb.rest.model.Node;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.providers.ServiceOutputHandler;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@Controller
public class DataWebservice {
    private static final Logger LOG = getLogger(DataWebservice.class);
    
    
    private final ModelLoaderFactory modelLoaderFactory;

    private final EngineManager engineManager;

    @Autowired
    public DataWebservice(ModelLoaderFactory modelLoaderFactory, EngineManager engineManager) {
        this.modelLoaderFactory = modelLoaderFactory;
        this.engineManager = engineManager;
    }


    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/**")
    public void getData(@RequestParam(value = "begin", defaultValue = "") String begin,
                                  @RequestParam(value = "top", defaultValue = "") String top,
                                  @RequestParam(value = "orderBy", defaultValue = "") String orderBy, HttpServletRequest request, HttpServletResponse servletResponse) {
        String path = request.getRequestURI();
        StatRecord serviceRecord = StatisticsMonitor.createRecord("service:request");
        try {
            String pathInfo = URLDecoder.decode(path, "UTF8");
            LOG.debug("Requested path: {}", pathInfo);

            PathParser parser = new PathParser(pathInfo);
            List<OrderParam> orderParamList = OrderParameterParsing.getOrderParams(orderBy);
            RestEntity entity = null;
            InputElement element = parser.next();
            while(element != null) {
                PathModelLoader loader = modelLoaderFactory.getModelLoader(element.getElementName());
                LOG.debug("Input element: {} using loader: {}", element, loader);
                entity = loader.loadModel(element, begin, top, orderParamList, getRequestContext(request));
                element.setResult(entity);

                if(parser.hasNext()) {
                    element = parser.next();

                    if(loader.isOperationSupported(element.getElementName())) {
                        RestEntity operationResult = loader.doOperation(element);
                        if(operationResult != null) {
                            entity = operationResult;
                        }
                        element = parser.next();
                    }
                } else {
                    element = null;
                }
            }

            ServiceOutputHandler.createResponse(entity, servletResponse);
        } catch(RestException e) {
            LOG.trace("Invalid syntax stack:", e);
            LOG.debug("Invalid syntax:", e.getMessage());

            ServiceOutputHandler.handleError("Unable to load model: " + e.getMessage(), servletResponse);
        } catch(UnsupportedEncodingException e) {
            LOG.error("Invalid path, encoding invalid: " + path, e);
            ServiceOutputHandler.handleError("Invalid path, not UTF8 encoded: " + e.getMessage(), servletResponse);
        } finally {
            serviceRecord.stop();
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "/", produces = "application/json")
    public @ResponseBody Node getNodeInformation() {
        return NodeInfoMapper.mapTo(engineManager.getNodeInformation());
    }

    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/**", consumes = "application/json", produces = "application/json")
    public void handlePostData(HttpServletRequest request, HttpServletResponse servletResponse, @RequestBody String data) {
        String pathInfo = request.getRequestURI();
        LOG.debug("Path: {}", pathInfo);
        StatRecord writeHandleRecord = StatisticsMonitor.createRecord("ws:handlepost");
        try {
            LOG.debug("Received data: {}", data);
            try {
                InputElement lastElement = processPath(pathInfo, getRequestContext(request));

                if(lastElement != null) {
                    PathModelLoader loader = modelLoaderFactory.getModelLoader(lastElement.getElementName());
                    RestEntity entity = loader.writeEntry(lastElement, ServiceOutputHandler.getResponseHandler(), data, getRequestContext(request));

                    ServiceOutputHandler.createResponse(entity, servletResponse);
                } else {
                    throw new SyntaxException("No path specified for write operation: " + pathInfo);
                }
            } catch(JasDBStorageException | RestException e) {
                LOG.trace("Invalid write operation", e);

                ServiceOutputHandler.handleError("Unable to write to model: " + e.getMessage(), servletResponse);
            }
        } finally {
            writeHandleRecord.stop();
        }
    }

    private InputElement processPath(String pathInfo, RequestContext requestContext) throws JasDBStorageException, RestException {
        PathParser parser = new PathParser(pathInfo);
        InputElement lastElement = null;
        List<OrderParam> emptyOrderParams = Collections.emptyList();
        for(InputElement element : parser) {
            lastElement = element;
            if(element.getNext() != null) {
                PathModelLoader loader = modelLoaderFactory.getModelLoader(lastElement.getElementName());
                element.setResult(loader.loadModel(element, null, "", emptyOrderParams, requestContext));
            }
        }

        return lastElement;
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/**", consumes = "application/json", produces = "application/json")
    public ResponseEntity<?> handleRemoveData(HttpServletRequest request, @RequestBody String data) {
        String path = request.getRequestURI();
        LOG.debug("Removing for path: {}", path);
        StatRecord removeHandleRecord = StatisticsMonitor.createRecord("ws:handleRemove");
        try {
            try {
                InputElement lastElement = processPath(path, getRequestContext(request));
                if(lastElement != null) {
                    PathModelLoader loader = modelLoaderFactory.getModelLoader(lastElement.getElementName());
                    loader.removeEntry(lastElement, ServiceOutputHandler.getResponseHandler(), data, getRequestContext(request));

                    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
                } else {
                    throw new SyntaxException("No path specified for remove operation: " + path);
                }
            } catch(RestException | JasDBStorageException e) {
                return new ResponseEntity<Object>(new ErrorEntity(400, "Unable to remove entity"), HttpStatus.BAD_REQUEST);
            }
        } finally {
            removeHandleRecord.stop();
        }
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/**", consumes = "application/json", produces = "application/json")
    public void handleUpdateData(HttpServletRequest request, HttpServletResponse response, @RequestBody String data) {
        String pathInfo = request.getRequestURI();
        StatRecord removeHandleRecord = StatisticsMonitor.createRecord("ws:handleUpdate");
        try {
            try {
                InputElement lastElement = processPath(pathInfo, getRequestContext(request));
                if(lastElement != null) {
                    PathModelLoader loader = modelLoaderFactory.getModelLoader(lastElement.getElementName());
                    RestEntity entity = loader.updateEntry(lastElement, ServiceOutputHandler.getResponseHandler(), data, getRequestContext(request));

                    ServiceOutputHandler.createResponse(entity, response);
                } else {
                    throw new SyntaxException("No path specified for remove operation: " + pathInfo);
                }
            } catch(JasDBStorageException | RestException e) {
                ServiceOutputHandler.handleError("Unable to update entity: " + e.getMessage(), response);
            }
        } finally {
            removeHandleRecord.stop();
        }
    }

    private RequestContext getRequestContext(HttpServletRequest request) {
        boolean isClientRequest = RequestContextUtil.isClientRequest(request.getHeader("requestcontext"));

        RequestContext context = new RequestContext(isClientRequest, request.isSecure());
        UserSession session = (UserSession) request.getAttribute("session");
        context.setUserSession(session);

        return context;
    }
}
