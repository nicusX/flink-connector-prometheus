package org.apache.flink.connector.prometheus.sink;

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.prometheus.sink.errorhandling.OnErrorBehavior;
import org.apache.flink.connector.prometheus.sink.errorhandling.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.http.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.prometheus.Types;
import org.apache.flink.connector.prometheus.sink.testutils.TimeSeriesSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.ArrayList;
import java.util.List;

@ExtendWith(MiniClusterExtension.class)
public class PrometheusSinkITCase {


    private static final FixedHostPortGenericContainer<?> prometheusContainer = new FixedHostPortGenericContainer<>("prom/prometheus")

    ;

    public String getPrometheusEndpoint() {
        return "http://" + prometheusContainer.getHost() + ":" + prometheusContainer.getMappedPort(9090);
    }

    public String getPrometheusRemoteWrite() {
        return getPrometheusEndpoint()+"/api/v1/write";
    }
    @BeforeEach
    void setUp() throws Exception {
        prometheusContainer
                .withExposedPorts(9090)
                .withFixedExposedPort(9090,9090)
                .withFileSystemBind("/Users/fmorillo/Documents/AWS/github/flink-connector-prometheus/prometheus-connector/src/test/java/org/apache/flink/connector/prometheus/sink/testutils/prometheus.yml",
                        "/etc/prometheus/prometheus.yml", BindMode.READ_WRITE)
                .withCommand("--web.enable-remote-write-receiver","--config.file=/etc/prometheus/prometheus.yml")
                .waitingFor(Wait.forHttp("/"))
                .start();

    }

    @AfterEach
    void tearDown() throws Exception {
        Thread.sleep(500000);

        prometheusContainer.stop();
    }

    @Test
    void testPrometheusSinkIntegration_DataIngestedCorrectly() throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(100)
                .withPrometheusRemoteWriteEndpoint(getPrometheusRemoteWrite())
                .runScenario();
    }

    private static class Scenario {
        private int numberOfElementsToSend;
        private String prometheusRemoteWrite;

        public Scenario withNumberOfElementsToSend(int numberOfElementsToSend) {
            this.numberOfElementsToSend = numberOfElementsToSend;
            return this;
        }

        public Scenario withPrometheusRemoteWriteEndpoint(String prometheusRemoteWrite) {
            this.prometheusRemoteWrite = prometheusRemoteWrite;
            return this;
        }

        public void runScenario() throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            List<PrometheusTimeSeries> timeSeriesList = new ArrayList<>();

            System.out.println("Prometheus Web UI: " + prometheusRemoteWrite );

            // Create the PrometheusTimeSeries source with the specified number of elements
            TimeSeriesSourceFunction timeSeriesSource = new TimeSeriesSourceFunction(numberOfElementsToSend);

            DataStream<PrometheusTimeSeries> stream = env.addSource(timeSeriesSource);

            AsyncSinkBase<PrometheusTimeSeries, Types.TimeSeries> sink =
                    PrometheusSink.builder()
                            .setMaxBatchSizeInSamples(500)
                            .setMaxRecordSizeInSamples(500)
                            .setMaxTimeInBufferMS(5000)
                            .setRetryConfiguration(
                                    RetryConfiguration.builder()
                                            .setInitialRetryDelayMS(30L)
                                            .setMaxRetryDelayMS(5000L)
                                            .setMaxRetryCount(100)
                                            .build())
                            .setSocketTimeoutMs(5000)
                            .setPrometheusRemoteWriteUrl(prometheusRemoteWrite)
                            .setErrorHandlingBehaviourConfiguration(
                                    SinkWriterErrorHandlingBehaviorConfiguration.builder()
                                            .onPrometheusNonRetriableError(OnErrorBehavior.DISCARD_AND_CONTINUE)
                                            .onMaxRetryExceeded(OnErrorBehavior.DISCARD_AND_CONTINUE)
                                            .onHttpClientIOFail(OnErrorBehavior.DISCARD_AND_CONTINUE)
                                            .build())
                            .build();

            stream
                    .keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector())
                    .sinkTo(sink);




            env.execute("Flink Prometheus Sink Integration Test");
        }
    }
}
