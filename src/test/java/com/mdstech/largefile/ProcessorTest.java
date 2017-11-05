package com.mdstech.largefile;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProcessorTest {

    @Test
    public void testReadLargeDatafile() throws Exception {
        Processor processor = new Processor();
        processor.processLargeFile("/Users/srini/IdeaProjects/vertx_ms/samplesparkxml/datafiles/sampleusers.csv");
//        processor.processLargeFile("src/test/resources/sample.csv");
    }

}
