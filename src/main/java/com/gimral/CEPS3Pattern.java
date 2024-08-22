package com.gimral;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CEPS3Pattern {
    private static final Logger LOG = LoggerFactory.getLogger(CEPS3Pattern.class);

    private SourceFunction<TransactionEvent> source;
    private SinkFunction<TransactionEvent> sink;

    public CEPS3Pattern(SourceFunction<TransactionEvent> source, SinkFunction<TransactionEvent> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<TransactionEvent> input = env.addSource(source)
                .returns(TypeInformation.of(TransactionEvent.class));

//        input.print();

        Pattern<TransactionEvent, ?> type3TransactionsPattern = Pattern.<TransactionEvent>begin("begin")
                .where(SimpleCondition.of(value -> value.getType().equals("Remittance")))
                .where(SimpleCondition.of(value -> value.getAmount() >= 1000))
                .followedBy("matchAmount")
                .where(new IterativeCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent transactionEvent, Context<TransactionEvent> context) throws Exception {
                        for (TransactionEvent event : context.getEventsForPattern("begin")) {
                            if(Objects.equals(event.getAmount(), transactionEvent.getAmount()))
                                return true;
                        }
                        return false;
                    }
                })
                .timesOrMore(1)
                .within(Duration.ofDays(1));

        PatternStream<TransactionEvent> patternStream = CEP.pattern(input, type3TransactionsPattern).inProcessingTime();

        DataStream<TransactionEvent> matched = patternStream.select((PatternSelectFunction<TransactionEvent, TransactionEvent>) pattern -> {
            List<TransactionEvent> matched1 = pattern.get("matchAmount");
            return matched1.get(matched1.size()-1);
        });

//        matched.print();

        matched.addSink(sink);

        env.execute();
    }
}
