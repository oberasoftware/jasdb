package com.oberasoftware.jasdb.core.utils;

import com.oberasoftware.jasdb.api.exceptions.ReflectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ReflectionLoader {
	private static final Logger log = LoggerFactory.getLogger(ReflectionLoader.class);
	
	public static <T> T loadClass(Class<T> expectedSubtype, String className, Object[] args) throws ReflectionException {
		List<Class<?>> argumentTypes = new ArrayList<Class<?>>();
		for(Object argument : args) {
			argumentTypes.add(argument.getClass());
		}
		return loadClass(expectedSubtype, className, argumentTypes.toArray(new Class<?>[0]), args);
	}
	
	public static <T> T loadClass(Class<T> expectedSubtype, String className, 
			Class<?>[] constructorArguments, Object[] args) throws ReflectionException {
		try {
			Class<? extends T> typeClass = Class.forName(className).asSubclass(expectedSubtype);
			
			Constructor<? extends T> constructor = typeClass.getConstructor(constructorArguments);
			return constructor.newInstance(args);
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalArgumentException | InstantiationException | IllegalAccessException | InvocationTargetException | SecurityException e) {
			log.error("Unable to load class", e);
			throw new ReflectionException("Unable to load class", e);
		}
    }

    public static Object invokeMethod(Object object, String method, Class<?>[] params, Object... args) throws ReflectionException {
        try {
            Method m = object.getClass().getDeclaredMethod(method, params);
            m.setAccessible(true);

            return m.invoke(object, args);
        } catch(NoSuchMethodException e) {
            log.error("Unable to invoke method, could not be found", e);
            throw new ReflectionException("Unable to invoke method, could not be found", e);
        } catch (InvocationTargetException e) {
            log.error("Unable to invoke target method, exception in method", e);
            throw new ReflectionException("Unable to invoke target method, exception in method", e);
        } catch (IllegalAccessException e) {
            log.error("Unable to invoke method, inaccessible", e);
            throw new ReflectionException("Unable to invoke method, inaccessible", e);
        }
    }

    public static Object getField(Object object, String field) throws ReflectionException {
        try {
            Field f = object.getClass().getDeclaredField(field);
            f.setAccessible(true);

            return f.get(object);
        } catch(NoSuchFieldException e) {
            log.error("Unable to invoke field, could not be found", e);
            throw new ReflectionException("Unable to invoke field, could not be found", e);
        } catch (IllegalAccessException e) {
            log.error("Unable to invoke method, inaccessible", e);
            throw new ReflectionException("Unable to invoke method, inaccessible", e);
        }
    }
}
