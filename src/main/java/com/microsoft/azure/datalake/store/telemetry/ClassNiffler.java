package com.microsoft.azure.datalake.store.telemetry;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Provides a test-friendly layer of indirection for reflection
 */
abstract class ClassNiffler {

    public static ClassNiffler getDefault() {
        return new ClassNiffler() {
            @Override
            public Class<?> forName(String name) throws ClassNotFoundException {
                return Class.forName(name);
            }
            @Override
            public Method getMethod(Class<?> cls, String name) throws NoSuchMethodException {
                return cls.getMethod(name);
            }
            @Override
            public Object invoke(Method method, Object obj, Object... args) throws InvocationTargetException, IllegalAccessException {
                return method.invoke(obj, args);
            }
        };
    }

    abstract public Class<?> forName(String name) throws ClassNotFoundException;

    abstract public Method getMethod(Class<?> cls, String name) throws NoSuchMethodException;

    abstract public Object invoke(Method method, Object obj, Object... args) throws InvocationTargetException, IllegalAccessException;
}
