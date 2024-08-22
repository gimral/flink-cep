package com.gimral;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

public class TransactionSource extends RichParallelSourceFunction<TransactionEvent> {
    private volatile boolean cancelled = false;

    private List<TransactionEvent> events;

    public TransactionSource(List<TransactionEvent> events){
        this.events = events;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<TransactionEvent> ctx) throws Exception {
        for (TransactionEvent event: events){
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(event);
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
