package nl.renarj.jasdb.index.keys.types;

/**
 * @author Renze de Vries
 */
public class ComplexKeyType implements KeyType {
    public static final String KEY_ID = "complexType";

    @Override
    public String getKeyId() {
        return KEY_ID;
    }

    @Override
    public String[] getKeyArguments() {
        return new String[0];
    }
}
