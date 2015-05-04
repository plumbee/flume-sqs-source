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

public final class ConfigurationConstants {

    private ConfigurationConstants() {
        // Prevent instantiation
    }

    /**
     * The SQS Queue URL e.g. https://sqs.us-east-1.amazonaws.com/1234567891/dev
     * This is the only mandatory configuration parameter!
     */
    public static final String CONFIG_QUEUE_URL = "url";

    /**
     * The maximum number of messages to retrieve from the SQS queue using the
     * receiveMessage() API call.
     */
    public static final String CONFIG_RECV_BATCH_SIZE = "recvBatchSize";
    public static final int DEFAULT_RECV_BATCH_SIZE = 10;

    /**
     * The maximum number of messages to delete from the SQS queue using the
     * deleteMessageBatch() API call.
     */
    public static final String CONFIG_DELETE_BATCH_SIZE = "deleteBatchSize";
    public static final int DEFAULT_DELETE_BATCH_SIZE = 10;

    /**
     * Support for Amazon SQS long polling. This option specifies the maximum
     * amount of time in seconds to wait for messages to become available in
     * the Queue before returning control to the callee. Setting this too low
     * could result in lots of empty responses which in turn increases cost.
     */
    public static final String CONFIG_RECV_TIMEOUT = "recvTimeout";
    public static final int DEFAULT_RECV_TIMEOUT = 20;

    /**
     * The number of seconds to wait before a message becomes visible again
     * after being retrieved without any subsequent deletion.
     */
    public static final String CONFIG_RECV_VISTIMEOUT = "recvVisabilityTimeout";
    public static final int DEFAULT_RECV_VISTIMEOUT = 3600;

    /**
     * The AWS access key Id and secret required to access the Queue. If not
     * defined the value will be determined from environment variables, then
     * Java properties and then IAM EC2 roles.
     */
    public static final String CONFIG_AWS_ACCESS_KEY_ID = "awsAccessKeyId";
    public static final String CONFIG_AWS_SECRET_KEY = "awsSecretKey";

    /**
     * Role ARN, Role Session Name and External Id for AWS Security Token Service to assume a Role and create temporary,
     * short-lived sessions to use for authentication.
     */
    public static final String CONFIG_AWS_ROLE_ARN = "awsRoleArn";
    public static final String CONFIG_AWS_ROLE_SESION_NAME = "awsRoleSessionName";
    public static final String CONFIG_AWS_EXTERNAL_ID = "awsExternalId";

    /**
     * The number of messages to extract from the queue before committing to
     * the downstream channel.
     */
    public static final String CONFIG_BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    /**
     * The maximum time in seconds between committing events to the downstream
     * channel if the batchSize has not been reached. Setting this value higher
     * than the recvVisabilityTimeout could potentially result in duplicate
     * messages being written to the channel.
     */
    public static final String CONFIG_FLUSH_INTERVAL = "flushInterval";
    public static final long DEFAULT_FLUSH_INTERVAL = 1800;

    /**
     * The maximum time in milliseconds to wait between SQS polls if no
     * messages are available in the queue or an exception occurs.
     */
    public static final String CONFIG_MAX_BACKOFF_SLEEP = "maxBackOffSleep";
    public static final long DEFAULT_MAX_BACKOFF_SLEEP = 20000;

    /**
     * The amount of time in milliseconds to increment the backoff sleep
     * delay in response to no messages being available.
     */
    public static final String CONFIG_BACKOFF_SLEEP_INCREMENT =
        "backOffSleepIncrement";
    public static final long DEFAULT_BACKOFF_SLEEP_INCREMENT = 1000;

    /**
     * The number of consumer threads to use when retrieving messages from SQS.
     */
    public static final String CONFIG_NB_CONSUMER_THREADS = "nbThreads";
    public static final int DEFAULT_NB_CONSUMER_THREADS = 5;
}
