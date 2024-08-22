package com.gimral;

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlinkCepAppS3 {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCepAppS3.class);

    public static void main(String[] args) throws Exception {

//        LOG.info("Starting");
//
//        List<TransactionEvent> transactionEvents = new ArrayList<>();
//        transactionEvents.add(new TransactionEvent(1000L, "1"));
//        transactionEvents.add(new TransactionEvent(3000L, "3"));
//        transactionEvents.add(new TransactionEvent(4000L, "1"));
//        transactionEvents.add(new TransactionEvent(1000L, "2"));
//        transactionEvents.add(new TransactionEvent(5000L, "3"));
//        transactionEvents.add(new TransactionEvent(1000L, "4"));
//        TransactionSource transactionSource = new TransactionSource(transactionEvents);
//
//        // values are collected in a static variable
//        PrintSinkFunction collectSink = new PrintSinkFunction<>();
//
//        CEPS3Pattern flinkCepAppS3 = new CEPS3Pattern(transactionSource,collectSink);
//        flinkCepAppS3.execute();
    }
}
