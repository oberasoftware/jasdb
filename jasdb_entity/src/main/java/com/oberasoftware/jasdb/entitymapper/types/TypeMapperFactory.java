package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Renze de Vries
 */
public class TypeMapperFactory {
    private static List<TypeMapper> typeMappers = new ArrayList<>();
    static {
        typeMappers.add(new StringTypeMapper());
        typeMappers.add(new SetEntityMapper());
        typeMappers.add(new LongTypeMapper());
        typeMappers.add(new MapEntityMapper());
        typeMappers.add(new ListEntityMapper());
        typeMappers.add(new EnumTypeMapper());
    }

    private TypeMapperFactory() {

    }

    public static TypeMapper getTypeMapper(Method method) throws JasDBStorageException {
        Class<?> returnType = method.getReturnType();
        return getTypeMapper(returnType);
    }

    public static <T> TypeMapper<T> getTypeMapper(Class<T> type) throws JasDBStorageException {
        Optional<TypeMapper> typeMapperOptional = typeMappers.stream().filter(t -> t.isSupportedType(type)).findAny();
        if(typeMapperOptional.isPresent()) {
            return typeMapperOptional.get();
        } else {
            throw new JasDBStorageException("Unable to find type mapper for return type: " + type);
        }
    }
}
