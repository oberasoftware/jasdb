package nl.renarj.core.exceptions;

public class FileException extends Exception {
	private static final long serialVersionUID = -15232595283101969L;

	public FileException(String exceptionMessage, Throwable embeddedException) {
		super(exceptionMessage, embeddedException);
	}
	
	public FileException(String exceptionMessage){
		super(exceptionMessage);
	}

}
