package org.apache.flink.connector.prometheus.sink.testutils;

import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class TimeSeriesSourceFunction implements SourceFunction<PrometheusTimeSeries> {

    private volatile boolean running = true;
    private final int numberOfTimeSeries;
    public TimeSeriesSourceFunction(int numberOfTimeSeries) {
        this.numberOfTimeSeries = numberOfTimeSeries;
    }
    @Override
    public void run(SourceContext<PrometheusTimeSeries> ctx) throws Exception {
        for (int i = 0; i < numberOfTimeSeries && running; i++) {
            long timestamp = System.currentTimeMillis();
            PrometheusTimeSeries timeSeries = PrometheusTimeSeries.builder()
                    .withMetricName("metric_name")
                    .addLabel("dimension_a", "anotherValueB")
                    .addSample(i, System.currentTimeMillis())
                    .build();
            ctx.collect(timeSeries);
            Thread.sleep(10000);
            }

        }

    @Override
    public void cancel() {
        running = false;
    }
}