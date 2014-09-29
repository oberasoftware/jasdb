/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.RemoteConnector;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.ErrorEntity;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

/**
 * User: renarj
 * Date: 1/23/12
 * Time: 9:39 PM
 */
public class RemoteRestConnector implements RemoteConnector {
    private static final Logger log = LoggerFactory.getLogger(RemoteRestConnector.class);

    protected static final String CHARACTER_ENCODING = "UTF8";
    protected enum REQUEST_MODE {
        GET,
        POST,
        DELETE,
        PUT
    }
    public static final String REQUESTCONTEXT = "requestcontext";

    
    private static final String CONNECTION_PROTOCOL_PROPERTY = "protocol";
    public static final String CONNECTION_HOST_PROPERTY = "host";
    public static final String CONNECTION_PORT_PROPERTY = "port";

    /* Connection properties */
    private String baseUrl;

    public RemoteRestConnector(NodeInformation nodeInformation) throws ConfigurationException {
        loadHostAddress(nodeInformation);
    }
    
    private void loadHostAddress(NodeInformation nodeInformation) throws ConfigurationException {
        Map<String, ?> remoteProperties = nodeInformation.getServiceInformation("rest").getNodeProperties();
        if(remoteProperties.containsKey(CONNECTION_HOST_PROPERTY) &&
                remoteProperties.containsKey(CONNECTION_PORT_PROPERTY) &&
                remoteProperties.containsKey(CONNECTION_PROTOCOL_PROPERTY)) {
            String host = (String)remoteProperties.get(CONNECTION_HOST_PROPERTY);
            String port = (String)remoteProperties.get(CONNECTION_PORT_PROPERTY);
            String protocol = (String) remoteProperties.get(CONNECTION_PROTOCOL_PROPERTY);

            if(remoteProperties.containsKey("verifyCert") && remoteProperties.get("verifyCert").equals("false")) {
                disableCertificationValidation();
            }

            if(protocol.equals("http") || protocol.equals("https")) {
                try {
                    int portNumber = Integer.parseInt(port);

                    this.baseUrl = protocol + "://" + host + ":" + portNumber + "/";
                    log.debug("Loaded rest connector with baseUrl: {}", baseUrl);
                } catch(NumberFormatException e) {
                    throw new ConfigurationException("Invalid Rest client port number: " + port);
                }
            } else {
                throw new ConfigurationException("Unsupported Rest client protocol: " + protocol);
            }
            
        } else {
            throw new ConfigurationException("Unable to load remote connection properties to establish rest client connection");
        }
    }

    private void disableCertificationValidation() throws ConfigurationException {
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                }
        };
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch(NoSuchAlgorithmException | KeyManagementException e) {
            throw new ConfigurationException("Unable to disable SSL verification", e);
        }

        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });
    }
    
    @Override
	public void close() {
	}

    private URL getUrl(String resource, Map<String, String> params) throws MalformedURLException, UnsupportedEncodingException {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(baseUrl).append(URLEncoder.encode(resource, "UTF8"));

        boolean first = true;
        for(Map.Entry<String, String> param : params.entrySet()) {
            char queryChar = first ? '?' : '&';
            first = false;
            urlBuilder.append(queryChar).append(param.getKey()).append("=").append(URLEncoder.encode(param.getValue(), "UTF8"));
        }
        return new URL(urlBuilder.toString());
    }

    protected byte[] toBytes(RestEntity entity) throws RestException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        new JsonRestResponseHandler().serialize(entity, bos);
        return bos.toByteArray();
    }

    protected ClientResponse doRequest(RemotingContext context, String connectionString) throws RemoteException {
        return doRequest(context, connectionString, new HashMap<String, String>());
    }

    protected ClientResponse doRequest(RemotingContext context, String connectionString, Map<String, String> params) throws RemoteException {
        return doInternalRequest(context, connectionString, params, null, REQUEST_MODE.GET);
    }

    protected ClientResponse doRequest(RemotingContext context, String connectionString, Map<String, String> params, String postBody, REQUEST_MODE mode) throws RemoteException {
        try {
            return doInternalRequest(context, connectionString, params, postBody != null ? postBody.getBytes("UTF8") : null, mode);
        } catch(UnsupportedEncodingException e) {
            throw new RemoteException("", e);
        }
    }
    
    protected ClientResponse doInternalRequest(RemotingContext context, String connectionString, Map<String, String> params, byte[] postStream, REQUEST_MODE mode) throws RemoteException {
        log.debug("Doing request to resource: {}", connectionString);

        HttpURLConnection urlConnection = null;
        try {
            URL url = getUrl(connectionString, params);
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod(mode.toString());
            urlConnection.setRequestProperty("Accept-Charset", "UTF-8");
            urlConnection.addRequestProperty("content-type", "application/json");
            urlConnection.setRequestProperty(REQUESTCONTEXT, getRequestContext(context));
            if(context.getUserSession() != null) {
                urlConnection.setRequestProperty("oauth_token", context.getUserSession().getAccessToken());
                urlConnection.setRequestProperty("sessionid", context.getUserSession().getSessionId());
            }

            if(postStream != null) {
                urlConnection.setDoOutput(true);
                urlConnection.getOutputStream().write(postStream);
            } else {
                urlConnection.connect();
            }
            return handleResponse(new ClientResponse(urlConnection.getInputStream(), urlConnection.getResponseCode()));
        } catch(MalformedURLException e) {
            throw new RuntimeJasDBException("The remote client url is invalid", e);
        } catch(UnsupportedEncodingException e) {
            throw new RemoteException("Unsupported encoding", e);
        } catch(IOException e) {
            if(urlConnection != null) {
                try {
                    return handleResponse(new ClientResponse(urlConnection.getErrorStream(), urlConnection.getResponseCode()));
                } catch(IOException ex) {
                    throw new RemoteException("Unable to connect to client", ex);
                }
            } else {
                throw new RemoteException("Unable to remote, fatal exception on connection", e);
            }
        }
    }

    private ClientResponse handleResponse(ClientResponse response) throws RemoteException {
        Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        if(status.getFamily() == Response.Status.Family.SUCCESSFUL) {
            return response;
        } else if(status.getFamily() == Response.Status.Family.CLIENT_ERROR) {
            String responseEntity = response.getEntityAsString();

            try {
                String message = "";
                if(responseEntity != null) {
                    ErrorEntity errorEntity = new JsonRestResponseHandler().deserialize(ErrorEntity.class, responseEntity);
                    message = errorEntity.getMessage();
                }

                if(status == Response.Status.NOT_FOUND) {
                    throw new ResourceNotFoundException("No resource was found, " + message);
                } else {
                    throw new RemoteException("Unable to execute remote operation: " + message + " statuscode: " + response.getStatus());
                }
            } catch(RestException e) {
                String reason = Response.Status.fromStatusCode(response.getStatus()).getReasonPhrase();
                throw new RemoteException("Unable to execute remote operation: " + response.getStatus() + "(" + reason + ")");
            }
        } else {
            String responseEntity = response.getEntityAsString();
            log.error("Remote server response with an error: {}", responseEntity);
            throw new RemoteException("Unable to execute remote operation: " + response.getStatus());
        }
    }

    private String getRequestContext(RemotingContext context) {
        if(context.isClientRequest()) {
            return "client";
        } else {
            return "grid";
        }
    }

    public String toString() {
        return baseUrl;
    }
}
