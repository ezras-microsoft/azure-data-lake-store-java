package com.microsoft.azure.datalake.store.telemetry;

/**
 * Where we can't figure out what kind of job context we're running in, or we know
 * it's one where we don't have a way to get the Job ID. Always returns null.
 */
final class NullIdFactory extends IdFactory {
    /**
     * Always returns null for the Job ID as there is no way to determine it
     * @return null
     */
    @Override
    public String getJobId() { return null; }

    /**
     * Always returns null for the compute engine name as there is no way to determine it
     * @return null
     */
    @Override
    public String getEngineName() { return null; }
}
