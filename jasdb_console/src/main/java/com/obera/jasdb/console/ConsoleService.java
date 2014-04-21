package com.obera.jasdb.console;

import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author renarj
 */
@Component
public class ConsoleService implements RemoteService {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleService.class);

    private Server server;

    private int portNr = 7052;

    private ServiceInformation serviceInformation;

    @Autowired
    private ApplicationContext applicationContext;

    public ConsoleService() {
        Map<String, String> serviceProperties = new HashMap<>();
        serviceProperties.put("connectorType", "web");
        serviceProperties.put("protocol", "http");
        serviceProperties.put("port", "" + portNr);

        this.serviceInformation = new ServiceInformation("console", serviceProperties);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void startService() throws JasDBException {
        LOG.info("Starting Web console");
        server = new Server(portNr);




        WebAppContext app = new WebAppContext();
        app.setContextPath("/");
        try {
            app.setWar(new ClassPathResource("webapp").getURI().toString());
        } catch(IOException e) {
            LOG.error("", e);
        }
//        XmlWebApplicationContext appContext = new XmlWebApplicationContext();
//        appContext.setParent(applicationContext);
//        appContext.setConfigLocation("dispatch-servlet.xml");
////        appContext.refresh();
//        app.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, appContext);

//        DispatcherServlet dispatcherServlet = new DispatcherServlet(appContext);
//        ServletHolder holder = new ServletHolder(dispatcherServlet);
//        holder.setInitOrder(1);

        server.setHandler(app);



//        ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
//        contextHandler.setContextPath("/");
//        try{
//            contextHandler.setResourceBase(new ClassPathResource("webapp").getURI().toString()); //this.getClass().getClassLoader().getResource("webapp").toExternalForm());
//
//        } catch(IOException e) {
//            LOG.error("", e);
//        }
//        server.setHandler(contextHandler);
//
//        XmlWebApplicationContext appContext = new XmlWebApplicationContext();
//        appContext.setParent(applicationContext);
////        appContext.setConfigLocation("META-INF/spring/applicationContext.xml");
////
//        ServletHolder holder = new ServletHolder("dispatch", new DispatcherServlet(appContext));
//
//        holder.setInitOrder(1);
//        contextHandler.addServlet(holder, "/*");

        try {
            LOG.info("Starting Console service on port: {}", portNr);
            server.start();
        } catch(Exception e) {
            LOG.error("Unable to start nodeInformation", e);
        }

    }

    @Override
    public void stopService() throws JasDBException {

    }

    @Override
    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }
}
