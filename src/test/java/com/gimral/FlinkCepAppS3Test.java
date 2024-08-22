package com.gimral;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class FlinkCepAppS3Test {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCepAppS3Test.class);
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testS3() throws Exception {
        LOG.info("Testing");
        List<TransactionEvent> transactionEvents = new ArrayList<>();
        transactionEvents.add(new TransactionEvent(1,1000L, "Payment"));
        transactionEvents.add(new TransactionEvent(2,3000L, "Remittance"));
        transactionEvents.add(new TransactionEvent(3,7000L, "Loan"));
        transactionEvents.add(new TransactionEvent(4,500L, "Remittance"));
        transactionEvents.add(new TransactionEvent(5,10000L, "Payment"));
        transactionEvents.add(new TransactionEvent(6,6000L, "Remittance"));
        transactionEvents.add(new TransactionEvent(7,4000L, "Payment"));
        transactionEvents.add(new TransactionEvent(8,500L, "Payment"));
        transactionEvents.add(new TransactionEvent(9,12000L, "Transfer"));
        transactionEvents.add(new TransactionEvent(10,3000L, "Remittance"));
        transactionEvents.add(new TransactionEvent(11,7000L, "Loan"));
        transactionEvents.add(new TransactionEvent(12,1000L, "Payment"));
        transactionEvents.add(new TransactionEvent(13,6000L, "Remittance"));
        TransactionSource transactionSource = new TransactionSource(transactionEvents);

        // values are collected in a static variable
        CollectSink collectSink = new CollectSink();

        CEPS3Pattern flinkCepAppS3 = new CEPS3Pattern(transactionSource,collectSink);
        flinkCepAppS3.execute();

        assertTrue(CollectSink.values.containsAll(Arrays.asList(10,13)));

    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<TransactionEvent> {

        // must be static
        public static final List<Integer> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(TransactionEvent value, SinkFunction.Context context) throws Exception {
            values.add(value.getId());
        }
    }


}
