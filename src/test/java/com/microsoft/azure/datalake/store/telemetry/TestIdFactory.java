package com.microsoft.azure.datalake.store.telemetry;

import com.contoso.helpers.HelperUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIdFactory {

    @Before
    public void setup() throws IOException {
        Properties prop = HelperUtils.getProperties();
        Assume.assumeTrue(Boolean.parseBoolean(prop.getProperty("MockTestsEnabled", "true")));
    }

    @Test
    public void testBuildsNullIdFactoryWhenClassesAreNotPresent() {
        IdFactory idFactory = IdFactory.get(new ClassNiffler() {
            @Override
            public Class<?> forName(String name) throws ClassNotFoundException {
                throw new ClassNotFoundException();
            }

            @Override
            public Method getMethod(Class<?> cls, String name) {
                return null;
            }

            @Override
            public Object invoke(Method method, Object obj, Object... args) {
                return null;
            }
        });
        assertTrue(idFactory instanceof NullIdFactory);
    }


    @Test
    public void testBuildsNullIdFactoryWhenClassesArePresentButNotMethods() {
        IdFactory idFactory = IdFactory.get(new ClassNiffler() {
            @Override
            public Class<?> forName(String name) {
                return null;
            }

            @Override
            public Method getMethod(Class<?> cls, String name) throws NoSuchMethodException {
                throw new NoSuchMethodException();
            }

            @Override
            public Object invoke(Method method, Object obj, Object... args) {
                return null;
            }
        });
        assertTrue(idFactory instanceof NullIdFactory);
    }

    @Test
    public void testBuildsNullIdFactoryWhenMethodInvocationFails() {
        IdFactory idFactory = IdFactory.get(new ClassNiffler() {
            @Override
            public Class<?> forName(String name) {
                return null;
            }

            @Override
            public Method getMethod(Class<?> cls, String name) {
                return null;
            }

            @Override
            public Object invoke(Method method, Object obj, Object... args) throws IllegalAccessException {
                throw new IllegalAccessException();
            }
        });
        assertTrue(idFactory instanceof NullIdFactory);
    }

    @Test
    public void testBuildsSparkIdFactoryWhenSparkClassesArePresent() {
        IdFactory idFactory = IdFactory.get(new ClassNiffler() {
            @Override
            public Class<?> forName(String name) {
                return null;
            }

            @Override
            public Method getMethod(Class<?> cls, String name) {
                return null;
            }

            @Override
            public Object invoke(Method method, Object obj, Object... args) {
                return "123";
            }
        });
        assertTrue(idFactory instanceof SparkIdFactory);
        assertEquals("123", idFactory.getJobId());
        assertEquals("spark", idFactory.getEngineName());
    }

    @Test
    public void testBuildsNullIdFactoryWhenSparkClassesFailToLoadMethods() {

    }

}
