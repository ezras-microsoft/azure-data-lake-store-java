package com.microsoft.azure.datalake.store.telemetry;

import java.lang.reflect.Method;

/**
 * If we're running in a Spark job (defined as having SparkEnv in the classpath),
 * this will attempt to get a Job ID. It is still not guaranteed to succeed on every call,
 * as it may be an old version of Spark (pre 1.4) or the application may not actually be a
 * Spark job despite having core Spark in the classpath. In those cases it returns null.
 *
 * Callers should expect to create a different implementation if the constructor throws,
 * possibly just NullIdFactory as a fallback.
 */
final class SparkIdFactory extends IdFactory {
    SparkIdFactory(ClassNiffler niffler) throws ReflectiveOperationException {
        Class<?> cSparkConf = niffler.forName("org.apache.spark.SparkConf");
        Class<?> cSparkEnv = niffler.forName("org.apache.spark.SparkEnv");
        Method mSparkEnvStaticGet = niffler.getMethod(cSparkEnv, "get");
        Method mSparkEnvConf = niffler.getMethod(cSparkEnv, "conf");
        Method mSparkConfGetAppId = niffler.getMethod(cSparkConf, "getAppId");
        Object oSparkEnv = niffler.invoke(mSparkEnvStaticGet, null);
        Object oSparkConf = niffler.invoke(mSparkEnvConf, oSparkEnv);
        _appId = (String) niffler.invoke(mSparkConfGetAppId, oSparkConf);
    }

    /**
     * Usually creates a Job ID based on SparkEnv.get().conf().getAppId()
     * but may return null if it can't get one. Note that Spark clusters will create the
     * Application ID automatically, but it CAN be set by the user, so uniqueness is not
     * actually guaranteed.
     * @return Job ID or null
     */
    @Override
    public String getJobId() {
        return _appId;
    }

    @Override
    public String getEngineName() {
        return "spark";
    }

    private String _appId;
}