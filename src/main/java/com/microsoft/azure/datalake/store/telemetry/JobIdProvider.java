package com.microsoft.azure.datalake.store.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobIdProvider {
    private static final Logger LOG =
            LoggerFactory.getLogger(JobIdProvider.class);

    /**
     * Package private setter to allow overriding the static IdFactory with an explicit choice
     * @param idFactory The IdFactory to use
     */
    static void init(IdFactory idFactory) {
        _idFactory = idFactory;
    }

    private static void init() {
        if (_idFactory == null) {
            _idFactory = IdFactory.get();
            LOG.debug("First invocation of getJobId, creating an IdFactory");
        }
    }

    /**
     * Provides an ID for the job on whose behalf it is called, or null if not available
     * Race conditions in calling this method are not a concern because the factory choice is
     * made purely on what code can be loaded
     * @return Job ID or null
     */
    public static String getJobId() {
        init();
        return _idFactory.getJobId();
    }

    public static String getEngineName() {
        init();
        return _idFactory.getEngineName();
    }

    private static IdFactory _idFactory = null;
}
