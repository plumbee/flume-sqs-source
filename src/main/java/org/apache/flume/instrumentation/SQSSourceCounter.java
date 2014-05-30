/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.instrumentation;

import java.util.concurrent.TimeUnit;

public class SQSSourceCounter
    extends MonitoredCounterGroup
    implements SQSSourceCounterMBean {

    // Batch Processing constants
    private static final String BATCH_RECEIVE_REQUEST_ATTEMPT_COUNT =
        "source.sqs.batch.receive.attempt.count";

    private static final String BATCH_RECEIVE_REQUEST_SUCCESS_COUNT =
        "source.sqs.batch.receive.success.count";

    private static final String BATCH_DELETE_REQUEST_ATTEMPT_COUNT =
        "source.sqs.batch.delete.attempt.count";

    private static final String BATCH_DELETE_REQUEST_SUCCESS_COUNT =
        "source.sqs.batch.delete.success.count";

    private static final String DELETE_MESSAGE_FAILED_COUNT =
        "source.sqs.delete.message.failed.count";

    private static final String DELETE_MESSAGE_SUCCESS_COUNT =
        "source.sqs.delete.message.success.count";

    // Configuration constants
    private static final String CONFIG_BATCH_REQUEST_SIZE =
        "source.config.batch.request.size";

    private static final String CONFIG_CONSUMER_THREAD_COUNT =
        "source.config.consumer.thread.count";

    // Event constants
    private static final String EVENT_RECEIVED_COUNT =
        "source.events.received.count";

    private static final String EVENT_REPROCESSED_COUNT =
        "source.events.reprocessed.count";

    // Polling constants
    private static final String RUNNER_BACKOFF_COUNT =
        "source.runner.backoff.count";

    private static final String RUNNER_CHANNEL_EXCEPTION_COUNT =
        "source.runner.channel.exception.count";

    private static final String RUNNER_DELIVERY_EXCEPTION_COUNT =
        "source.runner.delivery.exception.count";

    private static final String RUNNER_INTERRUPT_COUNT =
        "source.runner.interrupt.count";

    private static final String RUNNER_POLL_COUNT =
        "source.runner.poll.count";

    private static final String RUNNER_UNHANDLED_EXCEPTION_COUNT =
        "source.runner.unhandled.exception.count";

    // Statistics
    private long lastBatchEfficiencyUpdate = 0;
    private double prevBatchEfficiencyResult = 0.0;
    private long prevBatchReceiveRequestAttemptCount = 0;
    private long prevEventReceivedCount = 0;

    // Register all counters
    private static final String[] ATTRIBUTES = {
        BATCH_RECEIVE_REQUEST_ATTEMPT_COUNT,
        BATCH_RECEIVE_REQUEST_SUCCESS_COUNT,
        BATCH_DELETE_REQUEST_ATTEMPT_COUNT,
        BATCH_DELETE_REQUEST_SUCCESS_COUNT,
        DELETE_MESSAGE_FAILED_COUNT,
        DELETE_MESSAGE_SUCCESS_COUNT,
        CONFIG_BATCH_REQUEST_SIZE,
        CONFIG_CONSUMER_THREAD_COUNT,
        EVENT_RECEIVED_COUNT,
        EVENT_REPROCESSED_COUNT,
        RUNNER_BACKOFF_COUNT,
        RUNNER_CHANNEL_EXCEPTION_COUNT,
        RUNNER_DELIVERY_EXCEPTION_COUNT,
        RUNNER_INTERRUPT_COUNT,
        RUNNER_POLL_COUNT,
        RUNNER_UNHANDLED_EXCEPTION_COUNT
    };

    public SQSSourceCounter(String name) {
        super(MonitoredCounterGroup.Type.SOURCE, name, ATTRIBUTES);
    }

    // Batch processing methods
    @Override
    public double getBatchEfficiencyPercentage() {
        // Returns how efficient the batch SQS polling was at returning the
        // maximum number of messages per call. (Measured over the last
        // sampling period, cached for a minimum of 5 minutes)
        long currBatchReceiveRequestAttemptCount =
            getBatchReceiveRequestAttemptCount();
        long currEventReceivedCount = getEventReceivedCount();
        long currTimeMillis = System.currentTimeMillis();

        // Only update batch efficiency counter every 5 minutes.
        if (lastBatchEfficiencyUpdate > 0 && ((currTimeMillis -
            lastBatchEfficiencyUpdate) < TimeUnit.MINUTES.toMillis(5))) {
            return prevBatchEfficiencyResult;
        }
        double result = 100.0;
        if ((prevBatchReceiveRequestAttemptCount -
             currBatchReceiveRequestAttemptCount) != 0L) {
            result = (((currEventReceivedCount - prevEventReceivedCount) /
                (double)((currBatchReceiveRequestAttemptCount -
                    prevBatchReceiveRequestAttemptCount) *
                    getBatchRequestSize())) * 100);
        }
        lastBatchEfficiencyUpdate = currTimeMillis;
        prevBatchEfficiencyResult = result;
        prevBatchReceiveRequestAttemptCount =
            currBatchReceiveRequestAttemptCount;
        prevEventReceivedCount = currEventReceivedCount;
        return result;
    }

    @Override
    public long getBatchDeleteRequestAttemptCount() {
        return get(BATCH_DELETE_REQUEST_ATTEMPT_COUNT);
    }

    public long incrementBatchDeleteRequestAttemptCount() {
        return increment(BATCH_DELETE_REQUEST_ATTEMPT_COUNT);
    }

    @Override
    public long getBatchDeleteRequestSuccessCount() {
        return get(BATCH_DELETE_REQUEST_SUCCESS_COUNT);
    }

    public long incrementBatchDeleteRequestSuccessCount() {
        return increment(BATCH_DELETE_REQUEST_SUCCESS_COUNT);
    }

    @Override
    public long getDeleteMessageFailedCount() {
        return get(DELETE_MESSAGE_FAILED_COUNT);
    }

    public long addToDeleteMessageFailedCount(long delta) {
        return addAndGet(DELETE_MESSAGE_FAILED_COUNT, delta);
    }

    @Override
    public long getDeleteMessageSuccessCount() {
        return get(DELETE_MESSAGE_SUCCESS_COUNT);
    }

    public long addToDeleteMessageSuccessCount(long delta) {
        return addAndGet(DELETE_MESSAGE_SUCCESS_COUNT, delta);
    }

    @Override
    public long getBatchReceiveRequestAttemptCount() {
        return get(BATCH_RECEIVE_REQUEST_ATTEMPT_COUNT);
    }

    public long incrementBatchReceiveRequestAttemptCount() {
        return increment(BATCH_RECEIVE_REQUEST_ATTEMPT_COUNT);
    }

    @Override
    public long getBatchReceiveRequestSuccessCount() {
        return get(BATCH_RECEIVE_REQUEST_SUCCESS_COUNT);
    }

    public long incrementBatchReceiveRequestSuccessCount() {
        return increment(BATCH_RECEIVE_REQUEST_SUCCESS_COUNT);
    }

    // Configuration methods
    @Override
    public long getBatchRequestSize() {
        return get(CONFIG_BATCH_REQUEST_SIZE);
    }

    public void setBatchRequestSize(long batchRequestSize) {
        set(CONFIG_BATCH_REQUEST_SIZE, batchRequestSize);
    }

    @Override
    public long getConsumerThreadCount() {
        return get(CONFIG_CONSUMER_THREAD_COUNT);
    }

    public void setConsumerThreadCount(long consumerThreadCount) {
        set(CONFIG_CONSUMER_THREAD_COUNT, consumerThreadCount);
    }

    // Event methods
    @Override
    public long getEventReceivedCount() {
        return get(EVENT_RECEIVED_COUNT);
    }

    public long addToEventReceivedCount(long delta) {
        return addAndGet(EVENT_RECEIVED_COUNT, delta);
    }

    @Override
    public long getEventReprocessedCount() {
        return get(EVENT_REPROCESSED_COUNT);
    }

    public long incrementEventReprocessedCount() {
        return increment(EVENT_REPROCESSED_COUNT);
    }

    // Polling methods
    @Override
    public long getRunnerBackoffCount() {
        return get(RUNNER_BACKOFF_COUNT);
    }

    public long incrementRunnerBackoffCount() {
        return increment(RUNNER_BACKOFF_COUNT);
    }

    @Override
    public long getRunnerChannelExceptionCount() {
        return get(RUNNER_CHANNEL_EXCEPTION_COUNT);
    }

    public long incrementRunnerChannelExceptionCount() {
        return increment(RUNNER_CHANNEL_EXCEPTION_COUNT);
    }

    @Override
    public long getRunnerDeliveryExceptionCount() {
        return get(RUNNER_DELIVERY_EXCEPTION_COUNT);
    }

    public long incrementRunnerDeliveryExceptionCount() {
        return increment(RUNNER_DELIVERY_EXCEPTION_COUNT);
    }

    @Override
    public long getRunnerInterruptCount() {
        return get(RUNNER_INTERRUPT_COUNT);
    }

    public long incrementRunnerInterruptCount() {
        return increment(RUNNER_INTERRUPT_COUNT);
    }

    @Override
    public long getRunnerExceptionCount() {
        // Returns a global accessor for the number of exceptions
        return (get(RUNNER_CHANNEL_EXCEPTION_COUNT) +
                get(RUNNER_DELIVERY_EXCEPTION_COUNT) +
                get(RUNNER_UNHANDLED_EXCEPTION_COUNT));
    }

    @Override
    public long getRunnerPollCount() {
        return get(RUNNER_POLL_COUNT);
    }

    public long incrementRunnerPollCount() {
        return increment(RUNNER_POLL_COUNT);
    }

    @Override
    public long getRunnerUnhandledExceptionCount() {
        return get(RUNNER_UNHANDLED_EXCEPTION_COUNT);
    }

    public long incrementRunnerUnhandledExceptionCount() {
        return increment(RUNNER_UNHANDLED_EXCEPTION_COUNT);
    }
}
