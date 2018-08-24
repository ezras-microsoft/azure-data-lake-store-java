package com.microsoft.azure.datalake.store.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Thread-safe singleton for acquiring job information reflectively
 */
public class JobInfoProvider {
    private static final Logger LOG =
            LoggerFactory.getLogger(JobInfoProvider.class);

    /**
     * Compute engine identifiers
     */
    private final String applicationId;
    private final String engineName;
    private static JobInfoProvider instance = null;

    /**
     * The compute engines that we will try to extract job information from, in order of declaration
     */
    private enum InfoFactory {
        /**
         * If we're running in a Spark job (defined as having SparkEnv in the classpath),
         * this will attempt to get an application Id through SparkEnv.getDefault().conf().getAppId()
         * It is still not guaranteed to succeed on every call, as it may be an old version of Spark (pre 1.4)
         * or the application may not actually be a Spark job despite having core Spark in the classpath.
         * In those cases it returns null.
         */
        SPARK {
            @Override
            JobInfoProvider build(ClassNiffler niffler) {
                LOG.debug("Trying to get job info for Spark");
                try {
                    Class<?> cSparkConf = niffler.forName("org.apache.spark.SparkConf");
                    Class<?> cSparkEnv = niffler.forName("org.apache.spark.SparkEnv");
                    Method mSparkEnvStaticGet = niffler.getMethod(cSparkEnv, "getDefault");
                    Method mSparkEnvConf = niffler.getMethod(cSparkEnv, "conf");
                    Method mSparkConfGetAppId = niffler.getMethod(cSparkConf, "getAppId");
                    Object oSparkEnv = niffler.invoke(mSparkEnvStaticGet, null);
                    Object oSparkConf = niffler.invoke(mSparkEnvConf, oSparkEnv);
                    String appId = (String) niffler.invoke(mSparkConfGetAppId, oSparkConf);
                    LOG.debug("Successfully got job info for Spark");
                    return new JobInfoProvider(appId, "spark");
                } catch (ReflectiveOperationException e) {
                    LOG.debug("Failed to get job info for Spark", e);
                    return null;
                }
            }
        }
        ;

        /**
         * Factory method to acquire job information for each possible compute engine
         * @param niffler implementation to be used in reflective construction
         * @return JobInfoProvider instance
         */
        abstract JobInfoProvider build(ClassNiffler niffler);
    }

    private JobInfoProvider(String applicationId, String engineName) {
        this.applicationId = applicationId;
        this.engineName = engineName;
    }

    /**
     * Thread-safe instance getter for singleton JobInfoProvider
     * @return JobInfoProvider instance
     */
    public static JobInfoProvider get() {
        return get(ClassNiffler.getDefault());
    }

    /**
     * Package private instance getter for test
     * @param niffler implementation to be used in reflective construction
     */
    static JobInfoProvider get(ClassNiffler niffler) {
        // double-checked locking for performant thread safety
        if (instance == null) {
            synchronized (JobInfoProvider.class) {
                if (instance == null) {
                    LOG.debug("First invocation of JobInfoProvider, creating an IdFactory");
                    for (InfoFactory infoFactory : InfoFactory.values()) {
                        instance = infoFactory.build(niffler);
                        if (instance != null) {
                            LOG.debug("Created: " + infoFactory.name());
                            return instance;
                        }
                    }
                    instance = new JobInfoProvider(null, null);
                }
            }
        }
        return instance;
    }

    /**
     * Provides an ID for the job on whose behalf it is called, or null if not available
     * @return Application id or null
     */
    public String getApplicationId() {
        return applicationId;
    }

    /**
     * Provides a name for the compute engine that is currently running, or null if not available
     * @return Engine name or null
     */
    public String getEngineName() {
        return engineName;
    }
}
