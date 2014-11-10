package nl.renarj.jasdb.api.properties;

/**
 * @author renarj
 */
public class BooleanValue implements Value {

    private boolean booleanValue;

    public BooleanValue(boolean booleanValue) {
        this.booleanValue = booleanValue;
    }

    @Override
    public Object getValue() {
        return booleanValue;
    }

    public boolean toBoolean() {
        return booleanValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BooleanValue that = (BooleanValue) o;

        if (booleanValue != that.booleanValue) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (booleanValue ? 1 : 0);
    }

    @Override
    public String toString() {
        return Boolean.toString(booleanValue);
    }
}
