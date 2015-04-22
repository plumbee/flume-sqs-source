/*
 * Copyright 2015 Plumbee Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.instrumentation;

/**
 * This interface represents an SQSSourceCounterMBean exposed via JMX
 */
public interface SQSSourceCounterMBean {

    // SQS Batch processing counters
    double getBatchEfficiencyPercentage();

    long getBatchDeleteRequestAttemptCount();

    long getBatchDeleteRequestSuccessCount();

    long getBatchReceiveRequestAttemptCount();

    long getBatchReceiveRequestSuccessCount();

    long getDeleteMessageFailedCount();

    long getDeleteMessageSuccessCount();

    // Configuration
    long getBatchRequestSize();

    long getConsumerThreadCount();

    // Event counters
    long getEventReceivedCount();

    long getEventReprocessedCount();

    // Polling counters
    long getRunnerBackoffCount();

    long getRunnerChannelExceptionCount();

    long getRunnerDeliveryExceptionCount();

    long getRunnerExceptionCount();

    long getRunnerInterruptCount();

    long getRunnerPollCount();

    long getRunnerUnhandledExceptionCount();

    // Lifecycle, inherited counters
    long getStartTime();

    long getStopTime();

    String getType();
}
