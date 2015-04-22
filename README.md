# Flume NG SQS Plugin

This project provides a [Flume NG](http://flume.apache.org/) source plugin for extracting messages from Amazon's Simple Queue Service ([SQS](http://aws.amazon.com/sqs/)), a fast, reliable, scalable and fully managed cloud based message queuing system.

## Installation

To get started, clone the repository and build the package (Requires Maven)

```
git clone https://github.com/plumbee/flume-sqs-source.git
cd flume-sqs-source
mvn package
```

Copy the resulting jar to a plugin specific lib directory:
```
sudo mkdir -p /usr/lib/flume-ng/plugins.d/flume-sqs-source/lib
sudo cp target/flume-sqs-source-1.0.0.jar /usr/lib/flume-ng/plugins.d/flume-sqs-source/lib/
```

And copy the AWS Java SDK dependencies to the plugin specific libext directory:
```
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get \
     -Dartifact=com.amazonaws:aws-java-sdk-core:1.9.6:jar \
     -Ddest=aws-java-sdk-core-1.9.6.jar \
     -Dtransitive=false
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get \
     -Dartifact=com.amazonaws:aws-java-sdk-sqs:1.9.6:jar \
     -Ddest=aws-java-sdk-sqs-1.9.6.jar \
     -Dtransitive=false
sudo mkdir -p /usr/lib/flume-ng/plugins.d/flume-sqs-source/libext
sudo cp aws-java-sdk-core-1.9.6.jar aws-java-sdk-sqs-1.9.6.jar /usr/lib/flume-ng/plugins.d/flume-sqs-source/libext/
```

## Configuration

The following configuration properties are supported by the plugin. Required properties are in **bold**

 Property Name         | Default | Description
-----------------------|---------|---------------------------------------------
 **channels**          | -       | The channels to be associated with the source.
 **type**              | -       | The component type name, needs to be *com.plumbee.flume.source.sqs.SQSSource*
 **queueUrl**          | -       | The SQS queue URL e.g. https://<span></span>sqs.us-east-1.amazonaws.com/1234567891/dev
 recvBatchSize         | 10      | The number of messages to download from SQS using a single call to receiveMessage(). Note: The current maximum (defined by Amazon) is 10.
 deleteBatchSize       | 10      | The number of messages to delete from SQS using a single call to deleteMessageBatch(). It is recommended that this value be the same as recvBatchSize to avoid unnecessary API calls.
 recvTimeout           | 20      | Specifies the maximum amount of time in seconds to wait for messages to become available in the queue before returning control to the callee. [Refer to Amazon SQS Long Polling](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html). Note: Setting this value too low can result in empty responses, increased API usage and cost.
 recvVisabilityTimeout | 3600    | The number of seconds to wait before a message previously taken from the queue (without deletion) becomes visible again to other consumers.
 awsAccessKeyId        |         | The AWS Access keyId to be used to authentication with SQS.
 awsSecretKey          |         | The Secret Key associated to AWS Access keyId.
 batchSize             | 100     | The number of messages to collect from the queue before committing them to the channels.
 flushInterval         | 1800    | The maximum time in seconds between commits to the channels if the number of buffered messages has not reached the batchSize. Setting this value higher than the recvVisabilityTimeout can result in message duplication.
 nbThreads             | 5       | The number of threads to be used when retrieving messages from SQS.

#### Example:

The following example demonstrates how to log messages from a SQS queue into the Flume NG log file.
```
a1.sources = r1
a1.sinks = k1
a1.channels = c1

a1.sources.r1.type = com.plumbee.flume.source.sqs.SQSSource
a1.sources.r1.channels = c1
a1.sources.r1.url = https://sqs.us-east-1.amazonaws.com/1234567891/dev
a1.sources.r1.recvBatchSize = 10
a1.sources.r1.recvTimeout = 20
a1.sources.r1.batchSize = 100
a1.sources.r1.nbThreads = 1
a1.sources.r1.flushInterval = 30

a1.channels.c1.type = memory
a1.channels.c1.capacity = 100000
a1.channels.c1.transactionCapacity = 100

a1.sinks.k1.type = logger
a1.sinks.k1.channel = c1

```

#### Notes:
* If the *awsAccessKeyId* and *awsSecretKey* properties are not defined the [Java AWS SDK](http://aws.amazon.com/sdkforjava/) will use a default credentials provider chain and search for credentials via:
 * Environment variables: *AWS_ACCESS_KEY_ID* and *AWS_SECRET_KEY*
 * Java System Properties: *aws.accessKeyId* and *aws.secretKey*
 * Credential profiles: ~/.aws/credentials
 * Instance profile credentials delivered through the Amazon EC2 metadata service


* Internally the plugin has a backoff mechanism which kicks in when the number of messages per batch request is less than 90% of the recvBatchSize. The backoff essentially slows down SQS polling by 1 second for each consecutive violation of the 90% rule up to a maximum of 20 seconds. So if the plugin makes 5 consecutive calls to SQS each returning 8 messages from a possible 10 then the plugin will wait 5 seconds before making another request. The consecutive increment and maximum backoff can be controlled by the *backOffSleepIncrement* and *maxBackOffSleep* properties defined in milliseconds. Note: If an error occurs, the plugin will immediately pause all activity for maxBackOffSleep milliseconds.


* To operate correctly the plugin requires *sqs:DeleteMessageBatch*, *sqs:DeleteMessage* and *sqs:ReceiveMessage* IAM privileges to be associated with the Instance profile or AWS Access KeyId, for example:

```json
{
  "Statement": [
    {
      "Sid": "AllowReceiveAndDeleteOperations",
      "Action": [
        "sqs:DeleteMessageBatch",
        "sqs:DeleteMessage",
        "sqs:ReceiveMessage"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:sqs:us-east-1:1234567891/dev"
      ]
    }
  ]
}
```

## Monitoring

The Flume NG SQS Plugin comes with a custom counter group implementation which can be queried using Jconsole or by enabling Flume's embedded [WebServer](https://flume.apache.org/FlumeUserGuide.html#json-reporting) to report the metrics in JSON. For example:

```json
{
    "SOURCE.sqs-1": {
        "RunnerInterruptCount": "0",
        "Type": "SOURCE",
        "RunnerChannelExceptionCount": "0",
        "EventReprocessedCount": "23306",
        "BatchReceiveRequestAttemptCount": "204151762",
        "DeleteMessageFailedCount": "18",
        "EventReceivedCount": "1886005718",
        "RunnerBackoffCount": "30265887",
        "BatchReceiveRequestSuccessCount": "204151762",
        "RunnerDeliveryExceptionCount": "0",
        "RunnerExceptionCount": "0",
        "DeleteMessageSuccessCount": "1886004112",
        "BatchEfficiencyPercentage": "93.33818663136995",
        "ConsumerThreadCount": "30",
        "RunnerUnhandledExceptionCount": "0",
        "StopTime": "0",
        "BatchDeleteRequestAttemptCount": "188600418",
        "RunnerPollCount": "204151774",
        "StartTime": "1399902496246",
        "BatchRequestSize": "10",
        "BatchDeleteRequestSuccessCount": "188600413"
    }
}
```

## License

Copyright 2015 Plumbee Ltd.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
