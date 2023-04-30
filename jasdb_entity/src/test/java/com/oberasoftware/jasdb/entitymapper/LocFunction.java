package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;

@JasDBEntity(bagName = "locFunction")
public class LocFunction {
    enum FUNCTION_TYPES {
        LIGHT,
        FRONT_REAR_LIGHT,
        CABIN_LIGHT
    }

    private int functionNumber;

    private FUNCTION_TYPES functionType;

    public LocFunction(int functionNumber, FUNCTION_TYPES functionType) {
        this.functionNumber = functionNumber;
        this.functionType = functionType;
    }

    @JasDBProperty
    public int getFunctionNumber() {
        return functionNumber;
    }

    public void setFunctionNumber(int functionNumber) {
        this.functionNumber = functionNumber;
    }

    @JasDBProperty
    public FUNCTION_TYPES getFunctionType() {
        return functionType;
    }

    public void setFunctionType(FUNCTION_TYPES functionType) {
        this.functionType = functionType;
    }
}
