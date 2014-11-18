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
package com.plumbee.flume.source.sqs;

import com.amazonaws.AbortedException;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SQSSourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchConsumer implements Runnable {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(BatchConsumer.class);

    private static final String SQS_ATTR_APPROXRECEIVECOUNT =
        "ApproximateReceiveCount";
    private static final String SQS_ATTR_SENTTIMESTAMP =
        "SentTimestamp";
    private static final long MAX_BACKOFF_SLEEP = 20000;

    private AmazonSQSClient client;
    private SQSSourceCounter sourceCounter;
    private ChannelProcessor channelProcessor;
    private DeleteMessageBatchRequest deleteMessageBatchRequest;
    private List<DeleteMessageBatchRequestEntry> batchDeleteRequestEntries;
    private List<Event> batchEventList;
    private ReceiveMessageRequest receiveMessageRequest;

    private String queueURL;
    private int queueDeleteBatchSize;
    private int queueRecvBatchSize;
    private int queueRecvPollingTimeout;
    private int queueRecvVisabilityTimeout;
    private int batchSize;
    private long flushInterval;
    private long consecutiveBackOffs;
    private long lastFlush;
    private long maxBackOffSleep;
    private long backOffSleepIncrement;

    public BatchConsumer(AmazonSQSClient client,
                         ChannelProcessor channelProcessor,
                         SQSSourceCounter sourceCounter,
                         String queueURL,
                         int queueDeleteBatchSize,
                         int queueRecvBatchSize,
                         int queueRecvPollingTimeout,
                         int queueRecvVisabilityTimeout,
                         int batchSize,
                         long flushInterval,
                         long maxBackOffSleep,
                         long backOffSleepIncrement) {
        this.client = client;
        this.channelProcessor = channelProcessor;
        this.sourceCounter = sourceCounter;
        this.queueURL = queueURL;
        this.queueDeleteBatchSize = queueDeleteBatchSize;
        this.queueRecvBatchSize = queueRecvBatchSize;
        this.queueRecvPollingTimeout = queueRecvPollingTimeout;
        this.queueRecvVisabilityTimeout = queueRecvVisabilityTimeout;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxBackOffSleep = maxBackOffSleep;
        this.backOffSleepIncrement = backOffSleepIncrement;

        consecutiveBackOffs = 0;
        lastFlush = System.currentTimeMillis();
    }

    @Override
    public void run() {

        // Initialize variables.
        receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setQueueUrl(queueURL);
        receiveMessageRequest.setWaitTimeSeconds(queueRecvPollingTimeout);
        receiveMessageRequest.setVisibilityTimeout(queueRecvVisabilityTimeout);

        receiveMessageRequest.withAttributeNames(SQS_ATTR_SENTTIMESTAMP);
        receiveMessageRequest.withAttributeNames(SQS_ATTR_APPROXRECEIVECOUNT);

        deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        deleteMessageBatchRequest.setQueueUrl(
            receiveMessageRequest.getQueueUrl());

        batchEventList = Lists.newArrayListWithCapacity(batchSize);
        batchDeleteRequestEntries = Lists.newArrayListWithCapacity(batchSize);

        // Process loop. Adapted from PollableSourceRunner, required to
        // bypass hardcoded values for maxBackOffSleep and
        // backOffSleepIncrement.
        while (!Thread.currentThread().isInterrupted()) {
            sourceCounter.incrementRunnerPollCount();
            try {
                if (process().equals(Status.BACKOFF)) {
                    sourceCounter.incrementRunnerBackoffCount();
                    consecutiveBackOffs++;
                    Thread.sleep(Math.min(consecutiveBackOffs *
                        backOffSleepIncrement, maxBackOffSleep));
                } else {
                    consecutiveBackOffs = 0;
                }
                continue;
            } catch (AbortedException e) {
                sourceCounter.incrementRunnerInterruptCount();
                Thread.currentThread().interrupt();
                break;
            } catch (InterruptedException e) {
                sourceCounter.incrementRunnerInterruptCount();
                Thread.currentThread().interrupt();
                break;
            } catch (EventDeliveryException e) {
                sourceCounter.incrementRunnerDeliveryExceptionCount();
                LOGGER.error("Unable to deliver event, sleeping for " +
                    MAX_BACKOFF_SLEEP + "ms", e);
            } catch (ChannelException e) {
                sourceCounter.incrementRunnerChannelExceptionCount();
                LOGGER.warn("Channel exception, sleeping for " +
                    MAX_BACKOFF_SLEEP + "ms", e);
            } catch (Exception e) {
                sourceCounter.incrementRunnerUnhandledExceptionCount();
                LOGGER.error("Unhandled exception, sleeping for " +
                    MAX_BACKOFF_SLEEP + "ms", e);
            }

            // An exception occurred, commence throttling (max penalty).
            try {
                Thread.sleep(MAX_BACKOFF_SLEEP);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        LOGGER.info("AmazonSQS consumer interrupted, {} messages in flight",
            batchDeleteRequestEntries.size());
    }

    public void flush() {

        // Commit messages to the downstream channel.
        LOGGER.debug("Flushing, transaction size: {}",
            batchDeleteRequestEntries.size());
        if (batchEventList.size() > 0) {
            channelProcessor.processEventBatch(batchEventList);
            batchEventList.clear();
        }

        // Request the batch deletion of messages.
        for (List<DeleteMessageBatchRequestEntry> partition :
            Lists.partition(batchDeleteRequestEntries,
                queueDeleteBatchSize)) {
            sourceCounter.incrementBatchDeleteRequestAttemptCount();
            deleteMessageBatchRequest.setEntries(partition);
            DeleteMessageBatchResult batchResult =
                client.deleteMessageBatch(deleteMessageBatchRequest);
            for (BatchResultErrorEntry errorEntry : batchResult.getFailed()) {
                LOGGER.error("Failed to delete message, {}",
                    errorEntry.toString());
            }
            sourceCounter.incrementBatchDeleteRequestSuccessCount();
            sourceCounter.addToDeleteMessageFailedCount(
                (long)batchResult.getFailed().size());
            sourceCounter.addToDeleteMessageSuccessCount(
                (long)batchResult.getSuccessful().size());
        }
        batchDeleteRequestEntries.clear();
        lastFlush = System.currentTimeMillis();
    }

    public Status process() throws EventDeliveryException {

        // Check if we've met the criteria to flush events.
        if (batchDeleteRequestEntries.size() >= batchSize) {
            flush();
        } else if ((flushInterval > 0) &&
            ((System.currentTimeMillis() - lastFlush) > flushInterval)) {
            flush();
        }

        // The number of messages pending insertion to the channels should
        // always by the same as the number of messages to delete from SQS!
        assert(batchEventList.size() ==
            batchDeleteRequestEntries.size());

        // Determine the maximum number of messages to request from SQS. We
        // never exceed the capacity of the internal buffers.
        if ((batchDeleteRequestEntries.size() + queueRecvBatchSize) >
            batchSize) {
            receiveMessageRequest.setMaxNumberOfMessages
                (batchSize - batchDeleteRequestEntries.size());
        } else {
            receiveMessageRequest.setMaxNumberOfMessages(queueRecvBatchSize);
        }

        // Retrieve messages.
        List<Message> messages = client.receiveMessage
            (receiveMessageRequest).getMessages();
        sourceCounter.incrementBatchReceiveRequestAttemptCount();
        for (Message message : messages) {

            // Extract SQS message attributes.
            long sentTimestamp = Long.parseLong
                (message.getAttributes().get(SQS_ATTR_SENTTIMESTAMP));
            long approximateReceiveCount = Long.parseLong
                (message.getAttributes().get(SQS_ATTR_APPROXRECEIVECOUNT));

            // Update statistics.
            if (approximateReceiveCount > 1) {
                sourceCounter.incrementEventReprocessedCount();
            }

            // By default the timestamp of the message is set to the
            // timestamp in UTC that the message was added to the SQS queue as
            // opposed to the time it was extracted.
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("timestamp", String.valueOf(sentTimestamp));
            batchEventList.add(EventBuilder.withBody
                (message.getBody(), Charsets.UTF_8, headers));
            batchDeleteRequestEntries.add(
                new DeleteMessageBatchRequestEntry()
                    .withId(Long.toString(batchEventList.size()))
                    .withReceiptHandle(message.getReceiptHandle()));
        }
        sourceCounter.incrementBatchReceiveRequestSuccessCount();
        sourceCounter.addToEventReceivedCount((long)messages.size());

        // If the payload was less than 90% of the maximum batch size,
        // instruct the runner to throttle polling.
        if (messages.size() < (queueRecvBatchSize * 0.9)) {
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    public static enum Status {
        READY, BACKOFF
    }
}
