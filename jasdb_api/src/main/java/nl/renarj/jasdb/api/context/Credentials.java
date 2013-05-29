package nl.renarj.jasdb.api.context;

/**
 * User: renarj
 * Date: 1/22/12
 * Time: 3:24 PM
 */
public interface Credentials {
    String getUsername();

    String getSourceHost();

    String getPassword();
}
