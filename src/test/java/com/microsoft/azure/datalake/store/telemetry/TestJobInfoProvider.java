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

    /**
     * Singletons should be reset between tests
     * This ensures that tests do not interfere with one another
     */
    private void resetJobInfoProviderSingleton() throws NoSuchFieldException, IllegalAccessException {
        Field instance = JobInfoProvider.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    /**
     * Scenario:
     *  No known compute engine classes are available
     * Expected result:
     *  1. A JobInfoFactory is constructed
     *  2. All of its job info is null
     */
    @Test
    public void testJobInfoProviderWorksWhenEngineClassGettingFails() {
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

    /**
     * Scenario:
     *  Attempts to get class methods fail
     * Expected result:
     *  1. A JobInfoFactory is constructed
     *  2. All of its job info is null
     */
    @Test
    public void testJobInfoProviderWorksWhenEngineMethodGettingFails() {
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

    /**
     * Scenario:
     *  Attempts to invoke class methods fail
     * Expected result:
     *  1. A JobInfoFactory is constructed
     *  2. All of its job info is null
     */
    @Test
    public void testJobInfoProviderWorksWhenEngineMethodInvocationFails() {
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

    /**
     * Scenario:
     *  The first compute engine (Spark) tried succeeds in loading classes, methods, and method invocation
     * Expected result:
     *  1. A JobInfoFactory is constructed
     *  2. All its job info is populated with values
     */
    @Test
    public void testJobInfoProviderWorksWhenSparkRequirementsArePresent() {
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
}
