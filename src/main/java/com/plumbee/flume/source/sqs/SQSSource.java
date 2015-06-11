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
package com.plumbee.flume.source.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SQSSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SQSSource
    extends AbstractSource
    implements Configurable, EventDrivenSource {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(SQSSource.class);

    private static final long FORCEFUL_TERMINATION_TIMEOUT = 30;

    private AmazonSQSClient client;
    private SQSSourceCounter sourceCounter;
    private ExecutorService consumerService;

    private String queueURL;
    private int queueDeleteBatchSize;
    private int queueRecvBatchSize;
    private int queueRecvPollingTimeout;
    private int queueRecvVisabilityTimeout;
    private int batchSize;
    private long flushInterval;
    private long maxBackOffSleep;
    private long backOffSleepIncrement;
    private int nbThreads;

    public SQSSource() {}

    // Used for unit testing.
    public SQSSource withAmazonSQSClient(AmazonSQSClient client) {
        this.client = client;
        return this;
    }

    // Used for unit testing.
    public SQSSource withSourceCounter(SQSSourceCounter sourceCounter) {
        this.sourceCounter = sourceCounter;
        return this;
    }

    @Override
    public void configure(Context context) {

        // Mandatory configuration parameters.
        queueURL = context.getString
            (ConfigurationConstants.CONFIG_QUEUE_URL);
        Preconditions.checkArgument(StringUtils.isNotBlank(queueURL),
            ErrorMessages.MISSING_MANDATORY_PARAMETER,
            ConfigurationConstants.CONFIG_QUEUE_URL);

        // Optional configuration parameters.
        queueRecvBatchSize = context.getInteger
            (ConfigurationConstants.CONFIG_RECV_BATCH_SIZE,
                ConfigurationConstants.DEFAULT_RECV_BATCH_SIZE);
        Preconditions.checkArgument(queueRecvBatchSize > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_RECV_BATCH_SIZE);

        queueDeleteBatchSize = context.getInteger
            (ConfigurationConstants.CONFIG_DELETE_BATCH_SIZE,
                ConfigurationConstants.DEFAULT_DELETE_BATCH_SIZE);
        Preconditions.checkArgument(queueDeleteBatchSize > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_DELETE_BATCH_SIZE);

        queueRecvPollingTimeout = context.getInteger
            (ConfigurationConstants.CONFIG_RECV_TIMEOUT,
                ConfigurationConstants.DEFAULT_RECV_TIMEOUT);
        Preconditions.checkArgument(queueRecvPollingTimeout > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_RECV_TIMEOUT);

        queueRecvVisabilityTimeout = context.getInteger
            (ConfigurationConstants.CONFIG_RECV_VISTIMEOUT,
                ConfigurationConstants.DEFAULT_RECV_VISTIMEOUT);
        Preconditions.checkArgument(queueRecvVisabilityTimeout > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_RECV_VISTIMEOUT);

        batchSize = context.getInteger
            (ConfigurationConstants.CONFIG_BATCH_SIZE,
                ConfigurationConstants.DEFAULT_BATCH_SIZE);
        Preconditions.checkArgument(batchSize > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_BATCH_SIZE);

        nbThreads = context.getInteger
            (ConfigurationConstants.CONFIG_NB_CONSUMER_THREADS,
                ConfigurationConstants.DEFAULT_NB_CONSUMER_THREADS);
        Preconditions.checkArgument(nbThreads > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_NB_CONSUMER_THREADS);
        Preconditions.checkArgument
            (nbThreads <= ClientConfiguration.DEFAULT_MAX_CONNECTIONS,
                "%s cannot cannot exceed %s " +
                    "(Default Amazon client connection pool size)",
                ConfigurationConstants.CONFIG_NB_CONSUMER_THREADS,
                ClientConfiguration.DEFAULT_MAX_CONNECTIONS);

        // Don't let the number of messages to be polled from SQS using one
        // call exceed the transaction batchSize for the downstream channel.
        Preconditions.checkArgument(queueRecvBatchSize <= batchSize,
            "%s must be smaller than or equal to the %s",
            ConfigurationConstants.CONFIG_RECV_BATCH_SIZE,
            ConfigurationConstants.CONFIG_BATCH_SIZE);

        flushInterval = context.getLong
            (ConfigurationConstants.CONFIG_FLUSH_INTERVAL,
                ConfigurationConstants.DEFAULT_FLUSH_INTERVAL);
        Preconditions.checkArgument(flushInterval > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_FLUSH_INTERVAL);
        flushInterval = TimeUnit.SECONDS.toMillis(flushInterval);

        // Runner backoff configuration.
        maxBackOffSleep = context.getLong
            (ConfigurationConstants.CONFIG_MAX_BACKOFF_SLEEP,
                ConfigurationConstants.DEFAULT_MAX_BACKOFF_SLEEP);
        Preconditions.checkArgument(maxBackOffSleep > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_MAX_BACKOFF_SLEEP);

        backOffSleepIncrement = context.getLong
            (ConfigurationConstants.CONFIG_BACKOFF_SLEEP_INCREMENT,
                ConfigurationConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
        Preconditions.checkArgument(backOffSleepIncrement > 0,
            ErrorMessages.NEGATIVE_PARAMETER_VALUE,
            ConfigurationConstants.CONFIG_BACKOFF_SLEEP_INCREMENT);

        Preconditions.checkArgument(flushInterval > maxBackOffSleep,
            "%s too high, %s cannot be respected",
            ConfigurationConstants.CONFIG_MAX_BACKOFF_SLEEP,
            ConfigurationConstants.CONFIG_FLUSH_INTERVAL);

        // Log a warning if the flushInterval plus maxBackOffSleep exceed
        // the queueRecvVisabilityTimeout of messages. On queues with
        // low levels of throughput this can cause message duplication!
        if ((flushInterval + maxBackOffSleep) >
            TimeUnit.SECONDS.toMillis(queueRecvVisabilityTimeout)) {
            LOGGER.warn("{} too low, potential for message duplication",
                ConfigurationConstants.CONFIG_FLUSH_INTERVAL);
        }

        if (client != null) {
            LOGGER.info("AmazonSQSClient already initialized, ignoring " +
               "AWS credential configuration parameters");
            return;
        }

        // The following configuration options allows credentials to be
        // provided via the configuration context.
        String awsAccessKeyId = context.getString
            (ConfigurationConstants.CONFIG_AWS_ACCESS_KEY_ID);
        String awsSecretKey = context.getString
            (ConfigurationConstants.CONFIG_AWS_SECRET_KEY);
        String awsRoleArn = context.getString
            (ConfigurationConstants.CONFIG_AWS_ROLE_ARN);
        String awsRoleSessionName = context.getString
            (ConfigurationConstants.CONFIG_AWS_ROLE_SESSION_NAME);
        String awsExternalId = context.getString
            (ConfigurationConstants.CONFIG_AWS_EXTERNAL_ID);

        // Determine the credential provider to use based on configuration
        // options. STS, Basic then Default.
        ClientConfiguration clientConfig = new ClientConfiguration()
            .withMaxConnections(nbThreads);
        if (StringUtils.isNotBlank(awsRoleArn) &&
            StringUtils.isNotBlank(awsRoleSessionName)) {
            STSAssumeRoleSessionCredentialsProvider.Builder stsBuilder =
                new STSAssumeRoleSessionCredentialsProvider.Builder(
                    awsRoleArn, awsRoleSessionName);
            if (StringUtils.isNotBlank(awsAccessKeyId) &&
                StringUtils.isNotBlank(awsSecretKey)) {
                stsBuilder.withLongLivedCredentials(
                    new BasicAWSCredentials(awsAccessKeyId, awsSecretKey));
            } else {
                stsBuilder.withLongLivedCredentialsProvider(
                    new DefaultAWSCredentialsProviderChain());
            }
            if (StringUtils.isNotBlank(awsExternalId)) {
                stsBuilder.withExternalId(awsExternalId);
            }
            LOGGER.info("Using STSAssumeRoleSessionCredentialsProvider to " +
                "request AWS credentials");
            client = new AmazonSQSClient(stsBuilder.build(), clientConfig);
        } else if (StringUtils.isNotBlank(awsAccessKeyId) &&
                   StringUtils.isNotBlank(awsSecretKey)) {
            LOGGER.info("Using BasicAWSCredentials to provide AWS credentials");
            client = new AmazonSQSClient(new BasicAWSCredentials(
                awsAccessKeyId, awsSecretKey), clientConfig);
        } else {
            LOGGER.info("Using DefaultAWSCredentialsProviderChain to request " +
                "AWS credentials");
            client = new AmazonSQSClient(
                new DefaultAWSCredentialsProviderChain(), clientConfig);
        }
    }

    @Override
    public void start() {
        LOGGER.info("Starting AmazonSQS {}...", getName());

        // Initialize the sourceCounter.
        if (sourceCounter == null) {
            sourceCounter = new SQSSourceCounter(getName());
        }
        sourceCounter.start();
        sourceCounter.setBatchRequestSize(queueRecvBatchSize);
        sourceCounter.setConsumerThreadCount(nbThreads);

        // Create the consumer threads.
        consumerService = Executors.newFixedThreadPool(nbThreads,
            new ThreadFactoryBuilder().setNameFormat(
                "sqs-consumer-" + getName() + "-%d")
                .build());
        for (int i = 0; i < nbThreads; i++) {
            consumerService.execute(
                new BatchConsumer(
                    client,
                    getChannelProcessor(),
                    sourceCounter,
                    queueURL,
                    queueDeleteBatchSize,
                    queueRecvBatchSize,
                    queueRecvPollingTimeout,
                    queueRecvVisabilityTimeout,
                    batchSize,
                    flushInterval,
                    maxBackOffSleep,
                    backOffSleepIncrement));
        }

        super.start();
        LOGGER.info("AmazonSQS source {} started", getName());
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping AmazonSQS source {}...", getName());

        // Shutdown the consumer service. As the consumers execute long running
        // tasks there is no point requesting a graceful shutdown so we just
        // go ahead and interrupt them.
        try {
            consumerService.shutdown();
            consumerService.shutdownNow();
            if (!consumerService.awaitTermination
                (FORCEFUL_TERMINATION_TIMEOUT, TimeUnit.SECONDS)) {
                LOGGER.warn("AmazonSQS consumers did not terminate");
            }
        } catch (InterruptedException e) {
            consumerService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (client != null) {
            client.shutdown();
        }
        if (sourceCounter != null) {
            sourceCounter.stop();
        }

        super.stop();
        LOGGER.info("AmazonSQS source {} stopped", getName());
    }
}
