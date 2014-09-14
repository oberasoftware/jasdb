package nl.renarj.core.utilities;

public class StringUtils {
	public static boolean stringNotEmpty(String string) {
        return !stringEmpty(string);
	}
	
	public static boolean stringEmpty(String string) {
        return string == null || string.isEmpty();
	}
}
