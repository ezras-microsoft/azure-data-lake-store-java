package com.microsoft.azure.datalake.store.telemetry;

import java.lang.reflect.Field;

class Util {
    /**
     * Singletons should be reset between tests
     * This ensures that tests do not interfere with one another
     */
    static void resetJobInfoProviderSingleton() throws NoSuchFieldException, IllegalAccessException {
        Field instance = JobInfoProvider.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }
}
