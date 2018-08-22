package com.microsoft.azure.datalake.store.telemetry;

import com.contoso.helpers.HelperUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestJobIdProvider {
    @Before
    public void setup() throws IOException, NoSuchFieldException, IllegalAccessException {
        Properties prop = HelperUtils.getProperties();
        Assume.assumeTrue(Boolean.parseBoolean(prop.getProperty("MockTestsEnabled", "true")));
        resetJobIdProviderSingleton();
    }

    private void resetJobIdProviderSingleton() throws NoSuchFieldException, IllegalAccessException {
        Field instance = JobIdProvider.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testJobIdProviderHandlesInabilityToInstantiateFactories() {
        JobIdProvider jobIdProvider = JobIdProvider.get(new ClassNiffler() {
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
        assertNull(jobIdProvider.getApplicationId());
        assertNull(jobIdProvider.getEngineName());
    }


    @Test
    public void testBuildsNullIdFactoryWhenClassesArePresentButNotMethods() {
        JobIdProvider jobIdProvider = JobIdProvider.get(new ClassNiffler() {
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
        assertNull(jobIdProvider.getApplicationId());
        assertNull(jobIdProvider.getEngineName());
    }

    @Test
    public void testBuildsNullIdFactoryWhenMethodInvocationFails() {
        JobIdProvider jobIdProvider = JobIdProvider.get(new ClassNiffler() {
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
        assertNull(jobIdProvider.getApplicationId());
        assertNull(jobIdProvider.getEngineName());
    }

    @Test
    public void testBuildsSparkIdFactoryWhenSparkClassesArePresent() {
        JobIdProvider jobIdProvider = JobIdProvider.get(new ClassNiffler() {
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
        assertEquals("123", jobIdProvider.getApplicationId());
        assertEquals("spark", jobIdProvider.getEngineName());
    }

    @Test
    public void testBuildsNullIdFactoryWhenSparkClassesFailToLoadMethods() {

    }
}
