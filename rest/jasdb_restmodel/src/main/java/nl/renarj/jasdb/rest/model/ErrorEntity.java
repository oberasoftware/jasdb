package nl.renarj.jasdb.rest.model;

/**
 * @author Renze de Vries
 *         Date: 8-6-12
 *         Time: 18:18
 */
public class ErrorEntity implements RestEntity {
    private int statusCode;
    private String message;

    public ErrorEntity(int statusCode, String message) {
        this.statusCode = statusCode;
        this.message = message;
    }

    public ErrorEntity() {

    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
