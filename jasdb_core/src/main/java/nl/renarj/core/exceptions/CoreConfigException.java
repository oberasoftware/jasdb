package nl.renarj.core.exceptions;

/**
 * This exception is thrown when an issue is found in the provided
 * configuration file
 *
 * @author Renze de Vries
 */
public class CoreConfigException extends Exception {
	private static final long serialVersionUID = -15232595283101969L;

	public CoreConfigException(String exceptionMessage, Throwable embeddedException) {
		super(exceptionMessage, embeddedException);
	}
	
	public CoreConfigException(String exceptionMessage){
		super(exceptionMessage);
	}

}
