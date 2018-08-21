package com.microsoft.azure.datalake.store.telemetry;

import com.contoso.helpers.HelperUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestJobIdProvider {
    @Before
    public void setup() throws IOException {
        Properties prop = HelperUtils.getProperties();
        Assume.assumeTrue(Boolean.parseBoolean(prop.getProperty("MockTestsEnabled", "true")));
    }

    @Test
    public void testJobIdProviderHandlesWorkingIdFactory() {
        JobIdProvider.init(new IdFactory() {
            @Override
            String getJobId() {
                return "jobId";
            }

            @Override
            String getEngineName() {
                return "engineName";
            }
        });
        assertEquals("jobId", JobIdProvider.getJobId());
        assertEquals("engineName", JobIdProvider.getEngineName());
    }

    @Test
    public void testJobIdProviderHandlesNullIdFactory() {
        JobIdProvider.init(new NullIdFactory());
        assertNull(JobIdProvider.getJobId());
        assertNull(JobIdProvider.getEngineName());
    }
}
