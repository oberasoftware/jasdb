package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Renze de Vries
 */
public class ClientResponse {
    private InputStream inputStream;
    private int status;

    public ClientResponse(InputStream inputStream, int status) {
        this.inputStream = inputStream;
        this.status = status;
    }

    public InputStream getEntityInputStream() {
        return inputStream;
    }

    public void close() {
        if(inputStream != null) {
            try {
                inputStream.close();
            } catch(IOException e) {
                throw new RuntimeJasDBException("Unable to cleanly close connection", e);
            }
        }
    }

    public String getEntityAsString() {
        if(inputStream != null) {
            try {
                BufferedReader read = new BufferedReader(new InputStreamReader(inputStream));
                String r = read.readLine();
                read.close();

                return r;
            } catch(IOException e) {
                throw new RuntimeJasDBException("Unable to load entity as string", e);
            }
        } else {
            return null;
        }
    }

    public int getStatus() {
        return status;
    }
}
