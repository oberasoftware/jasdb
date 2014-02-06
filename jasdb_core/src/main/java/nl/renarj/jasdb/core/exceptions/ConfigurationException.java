package nl.renarj.jasdb.core.exceptions;

/**
 * This exception is thrown when an issue is found in the provided
 * configuration file
 *
 * @author Renze de Vries
 */
public class ConfigurationException extends JasDBStorageException {
	private static final long serialVersionUID = -15232595283101969L;

	public ConfigurationException(String exceptionMessage, Throwable embeddedException) {
		super(exceptionMessage, embeddedException);
	}
	
	public ConfigurationException(String exceptionMessage){
		super(exceptionMessage);
	}

}
