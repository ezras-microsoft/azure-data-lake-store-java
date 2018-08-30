package com.microsoft.azure.datalake.store.telemetry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestJobInfoProviderSpark {
    private static final Logger LOG =
            LoggerFactory.getLogger(TestJobInfoProviderSpark.class);

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        // reset singleton
        Field instance = JobInfoProvider.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testJobIdProviderWorksWithSparkContextInitialized() {
        SparkConf conf = new SparkConf(false)
                .setAppName("Lucky App")
                .setMaster("local")
                .set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("Hearts", "Stars", "Horseshoes");
        JavaRDD<String> logData = sc.parallelize(data);
        LOG.info("Test data: " + logData.toString());
        JobInfoProvider x = JobInfoProvider.get();
        assertEquals("spark", x.getEngineName());
        assertNotNull(x.getApplicationId());
    }

    @Test
    public void testJobIdProviderWorksWithNoSparkContext() {
        JobInfoProvider x = JobInfoProvider.get();
        assertNull(x.getEngineName());
        assertNull(x.getApplicationId());
    }
}
