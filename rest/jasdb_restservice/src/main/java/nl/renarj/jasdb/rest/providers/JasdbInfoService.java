package nl.renarj.jasdb.rest.providers;

import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.locator.NodeInformation;
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
import nl.renarj.jasdb.rest.model.RestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.List;

@Path("/")
public class JasdbInfoService {
	private static final Logger log = LoggerFactory.getLogger(JasdbInfoService.class);
	
	@GET
	@Path("{pathinfo:.*}")
	@Produces(value={"application/json"}) //@HeaderParam("requestcontext") String requestContext,
	public Response getServiceInformation(@PathParam("pathinfo") String path, @Context HttpServletRequest request,
                                          @QueryParam("begin") @DefaultValue("") String begin,
                                          @QueryParam("top") @DefaultValue("") String top,
                                          @QueryParam("orderBy") @DefaultValue("") String orderBy) {

        StatRecord serviceRecord = StatisticsMonitor.createRecord("service:request");
		try {
            String pathInfo = URLDecoder.decode(path, "UTF8");
            log.debug("Requested path: {}", pathInfo);

            PathParser parser = new PathParser(pathInfo);
            List<OrderParam> orderParamList = OrderParameterParsing.getOrderParams(orderBy);
            RestEntity entity = null;
            InputElement element = parser.next();
            while(element != null) {
                PathModelLoader loader = SimpleKernel.getKernelModule(ModelLoaderFactory.class).getModelLoader(element.getElementName());
                log.debug("Input element: {} using loader: {}", element, loader);
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

            return ServiceOutputHandler.createResponse(entity);
		} catch(JasDBStorageException | RestException e) {
            log.trace("Invalid syntax stack:", e);
			log.debug("Invalid syntax:", e.getMessage());
			
            return ServiceOutputHandler.handleError("Unable to load model: " + e.getMessage());
		} catch(UnsupportedEncodingException e) {
            log.error("Invalid path, encoding invalid: " + path, e);
            return ServiceOutputHandler.handleError("Invalid path, not UTF8 encoded: " + e.getMessage());
        } finally {
            serviceRecord.stop();
        }
	}
    
    @GET
    @Produces(value={"application/json"})
    public Response getNodeInformation() {
        try {
            NodeInformation nodeInformationInfo = SimpleKernel.getNodeInformation();
            return ServiceOutputHandler.createResponse(NodeInfoMapper.mapTo(nodeInformationInfo));
        } catch(JasDBStorageException e) {
            log.error("Unable to load service information", e);
        }

        return ServiceOutputHandler.handleError("Unable to load node data");
    }

    @POST
	@Path("{pathinfo:.*}")
	@Consumes("application/json")
	@Produces
	public Response handlePostData(@PathParam("pathinfo") String pathInfo, @Context HttpServletRequest request, String data) {
        log.debug("Path: {}", pathInfo);
        StatRecord writeHandleRecord = StatisticsMonitor.createRecord("ws:handlepost");
        try {
            log.debug("Received data: {}", data);
            try {
                InputElement lastElement = processPath(pathInfo, getRequestContext(request));

                if(lastElement != null) {
                    PathModelLoader loader = SimpleKernel.getKernelModule(ModelLoaderFactory.class).getModelLoader(lastElement.getElementName());
                    RestEntity entity = loader.writeEntry(lastElement, ServiceOutputHandler.getResponseHandler(), data, getRequestContext(request));

                    return ServiceOutputHandler.createResponse(entity);
                } else {
                    throw new SyntaxException("No path specified for write operation: " + pathInfo);
                }
            } catch(JasDBStorageException | RestException e) {
                log.trace("Invalid write operation", e);

                return ServiceOutputHandler.handleError("Unable to write to model: " + e.getMessage());
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
                PathModelLoader loader = SimpleKernel.getKernelModule(ModelLoaderFactory.class).getModelLoader(lastElement.getElementName());
                element.setResult(loader.loadModel(element, null, "", emptyOrderParams, requestContext));
            }
        }

        return lastElement;
    }

    @DELETE
    @Path("{pathinfo:.*}")
    @Consumes("application/json")
    @Produces
    public Response handleRemoveData(@PathParam("pathinfo") String pathInfo, @Context HttpServletRequest request, String data) {
        log.debug("Removing for path: {}", pathInfo);
        StatRecord removeHandleRecord = StatisticsMonitor.createRecord("ws:handleRemove");
        try {
            try {
                InputElement lastElement = processPath(pathInfo, getRequestContext(request));
                if(lastElement != null) {
                    PathModelLoader loader = SimpleKernel.getKernelModule(ModelLoaderFactory.class).getModelLoader(lastElement.getElementName());
                    loader.removeEntry(lastElement, ServiceOutputHandler.getResponseHandler(), data, getRequestContext(request));

                    return Response.status(Response.Status.NO_CONTENT).type(ServiceOutputHandler.getResponseHandler().getMediaType()).build();
                } else {
                    throw new SyntaxException("No path specified for remove operation: " + pathInfo);
                }
            } catch(RestException | JasDBStorageException e) {
                return ServiceOutputHandler.handleError("Unable to remove in model: " + e.getMessage());
            }
        } finally {
            removeHandleRecord.stop();
        }
    }

    @PUT
    @Path("{pathinfo:.*}")
    @Consumes("application/json")
    @Produces
    public Response handleUpdateData(@PathParam("pathinfo") String pathInfo, @Context HttpServletRequest request, String data) {
        StatRecord removeHandleRecord = StatisticsMonitor.createRecord("ws:handleUpdate");
        try {
            try {
                InputElement lastElement = processPath(pathInfo, getRequestContext(request));
                if(lastElement != null) {
                    PathModelLoader loader = SimpleKernel.getKernelModule(ModelLoaderFactory.class).getModelLoader(lastElement.getElementName());
                    RestEntity entity = loader.updateEntry(lastElement, ServiceOutputHandler.getResponseHandler(), data, getRequestContext(request));

                    return ServiceOutputHandler.createResponse(entity);
                } else {
                    throw new SyntaxException("No path specified for remove operation: " + pathInfo);
                }
            } catch(JasDBStorageException | RestException e) {
                return ServiceOutputHandler.handleError("Unable to remove in model: " + e.getMessage());
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
