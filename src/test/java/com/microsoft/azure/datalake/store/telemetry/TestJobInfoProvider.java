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

public class TestJobInfoProvider {
    @Before
    public void setup() throws IOException, NoSuchFieldException, IllegalAccessException {
        Properties prop = HelperUtils.getProperties();
        Assume.assumeTrue(Boolean.parseBoolean(prop.getProperty("MockTestsEnabled", "true")));
        resetJobInfoProviderSingleton();
    }

    private void resetJobInfoProviderSingleton() throws NoSuchFieldException, IllegalAccessException {
        Field instance = JobInfoProvider.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testJobInfoProviderHandlesInabilityToInstantiateFactories() {
        JobInfoProvider jobInfoProvider = JobInfoProvider.get(new ClassNiffler() {
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
        assertNull(jobInfoProvider.getApplicationId());
        assertNull(jobInfoProvider.getEngineName());
    }


    @Test
    public void testBuildsNullIdFactoryWhenClassesArePresentButNotMethods() {
        JobInfoProvider jobInfoProvider = JobInfoProvider.get(new ClassNiffler() {
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
        assertNull(jobInfoProvider.getApplicationId());
        assertNull(jobInfoProvider.getEngineName());
    }

    @Test
    public void testBuildsNullIdFactoryWhenMethodInvocationFails() {
        JobInfoProvider jobInfoProvider = JobInfoProvider.get(new ClassNiffler() {
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
        assertNull(jobInfoProvider.getApplicationId());
        assertNull(jobInfoProvider.getEngineName());
    }

    @Test
    public void testBuildsSparkIdFactoryWhenSparkClassesArePresent() {
        JobInfoProvider jobInfoProvider = JobInfoProvider.get(new ClassNiffler() {
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
        assertEquals("123", jobInfoProvider.getApplicationId());
        assertEquals("spark", jobInfoProvider.getEngineName());
    }

    @Test
    public void testBuildsNullIdFactoryWhenSparkClassesFailToLoadMethods() {

    }
}
