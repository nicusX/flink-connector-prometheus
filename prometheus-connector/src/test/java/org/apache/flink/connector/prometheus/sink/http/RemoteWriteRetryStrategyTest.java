/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.http;

import org.apache.flink.connector.prometheus.sink.SinkMetrics;

import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class RemoteWriteRetryStrategyTest {

    private static final int INITIAL_RETRY_DELAY_MS = 30;
    private static final int MAX_RETRY_DELAY_MS = 5000;
    private static final RetryConfiguration RETRY_CONFIGURATION =
            RetryConfiguration.builder()
                    .setInitialRetryDelayMS(INITIAL_RETRY_DELAY_MS)
                    .setMaxRetryDelayMS(MAX_RETRY_DELAY_MS)
                    .setMaxRetryCount(Integer.MAX_VALUE)
                    .build();

    @Test
    public void shouldRetryOnRetriableErrorResponse() {
        var httpResponse = new BasicHttpResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        var httpContext = mock(HttpContext.class);
        var metrics = mock(SinkMetrics.class);

        var strategy = new RemoteWriteRetryStrategy(RETRY_CONFIGURATION, metrics);
        assertTrue(strategy.retryRequest(httpResponse, 1, httpContext));
    }

    @Test
    public void shouldNotRetryOnNonRetriableErrorResponse() {
        var httpResponse = new BasicHttpResponse(HttpStatus.SC_FORBIDDEN);
        var httpContext = mock(HttpContext.class);
        var metrics = mock(SinkMetrics.class);

        var strategy = new RemoteWriteRetryStrategy(RETRY_CONFIGURATION, metrics);
        assertFalse(strategy.retryRequest(httpResponse, 1, httpContext));
    }

    @Test
    public void shouldRetryIOException() {
        var httpRequest = mock(HttpRequest.class);
        var httpContext = mock(HttpContext.class);
        var metrics = mock(SinkMetrics.class);

        var strategy = new RemoteWriteRetryStrategy(RETRY_CONFIGURATION, metrics);

        assertTrue(strategy.retryRequest(httpRequest, new IOException("dummy"), 1, httpContext));
    }

    @Test
    public void shouldNotRetryNonRetriableIOExceptions() {
        var httpRequest = mock(HttpRequest.class);
        var httpContext = mock(HttpContext.class);
        var metrics = mock(SinkMetrics.class);

        var strategy = new RemoteWriteRetryStrategy(RETRY_CONFIGURATION, metrics);

        assertFalse(
                strategy.retryRequest(
                        httpRequest, new InterruptedIOException("dummy"), 1, httpContext));
        assertFalse(
                strategy.retryRequest(
                        httpRequest, new UnknownHostException("dummy"), 1, httpContext));
        assertFalse(
                strategy.retryRequest(httpRequest, new ConnectException("dummy"), 1, httpContext));
        assertFalse(
                strategy.retryRequest(
                        httpRequest, new NoRouteToHostException("dummy"), 1, httpContext));
        assertFalse(strategy.retryRequest(httpRequest, new SSLException("dummy"), 1, httpContext));
    }

    @Test
    public void retryDelayShouldDecreaseExponentiallyWithExecCount() {
        var httpResponse = mock(HttpResponse.class);
        var httpContext = mock(HttpContext.class);
        var counters = mock(SinkMetrics.class);

        var strategy = new RemoteWriteRetryStrategy(RETRY_CONFIGURATION, counters);

        assertEquals(
                TimeValue.ofMilliseconds(INITIAL_RETRY_DELAY_MS),
                strategy.getRetryInterval(httpResponse, 1, httpContext));
        assertEquals(
                TimeValue.ofMilliseconds(INITIAL_RETRY_DELAY_MS * 2),
                strategy.getRetryInterval(httpResponse, 2, httpContext));
        assertEquals(
                TimeValue.ofMilliseconds(INITIAL_RETRY_DELAY_MS * 2 * 2),
                strategy.getRetryInterval(httpResponse, 3, httpContext));
        assertEquals(
                TimeValue.ofMilliseconds(INITIAL_RETRY_DELAY_MS * 2 * 2 * 2),
                strategy.getRetryInterval(httpResponse, 4, httpContext));
    }

    @Test
    public void retryDelayShouldNotExceedMaximumDelay() {
        var retryConfiguration =
                RetryConfiguration.builder()
                        .setInitialRetryDelayMS(30)
                        .setMaxRetryDelayMS(5000)
                        .setMaxRetryCount(Integer.MAX_VALUE)
                        .build();
        var httpResponse = mock(HttpResponse.class);
        var httpContext = mock(HttpContext.class);
        var metrics = mock(SinkMetrics.class);

        var strategy = new RemoteWriteRetryStrategy(retryConfiguration, metrics);

        assertEquals(
                TimeValue.ofMilliseconds(5000),
                strategy.getRetryInterval(httpResponse, 10_000, httpContext));
    }
}
