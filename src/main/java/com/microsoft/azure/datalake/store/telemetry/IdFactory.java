package com.microsoft.azure.datalake.store.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * In general, a factory for getting the hopefully unique ID of the current job.
 * Implementations may return null always or sometimes.
 */
abstract class IdFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(IdFactory.class);

    /**
     * Factory method using the default methods for reflection
     * @return Fully constructed IdFactory, or NullIdFactory if nothing else can be constructed
     */
    public static IdFactory get() {
        return get(ClassNiffler.get());
    }

    /**
     * Factory method using specialized methods for reflection
     * @param niffler implements specialized methods for reflection, currently used for test
     * @return Fully constructed IdFactory, or NullIdFactory if nothing else can be constructed
     */
    static IdFactory get(ClassNiffler niffler) {
        try {
            IdFactory candidate = new SparkIdFactory(niffler);
            LOG.debug("Created a SparkIdFactory");
            return candidate;
        } catch (ReflectiveOperationException e) {
            LOG.debug("Could not construct a SparkIdFactory", e);
        }
        return new NullIdFactory();
    }

    /**
     * Get the job ID
     * @return Job ID or null
     */
    abstract String getJobId();

    /**
     * Get the name of the compute engine being used
     * @return Engine name or null
     */
    abstract String getEngineName();
}
