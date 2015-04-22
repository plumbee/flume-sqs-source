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

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SQSSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

public class SQSSourceTest {

    private static final int MAX_SENT_BATCH_SIZE = 10;
    private static final int MAX_SENT_BATCH_REQUESTS = 1000;

    private AbstractSource source;
    private SQSSourceCounter sourceCounter;
    private AtomicLong numDeletedMessages;
    private AtomicLong numSentMessages;
    private AtomicLong numSentBatchRequests;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {

        // Setup verification counters.
        numDeletedMessages = new AtomicLong();
        numSentMessages = new AtomicLong();
        numSentBatchRequests = new AtomicLong();

        // Create mock objects for simulating SQS interactions .
        AmazonSQSClient client = Mockito.mock(AmazonSQSClient.class);
        ReceiveMessageResult receiveMessageResult = Mockito.mock
            (ReceiveMessageResult.class);
        when(client.receiveMessage(isA(ReceiveMessageRequest.class)))
            .thenReturn(receiveMessageResult);
        when(receiveMessageResult.getMessages())
            .thenAnswer(receiveMessageResultCallback());
        when(client.deleteMessageBatch(isA(DeleteMessageBatchRequest.class)))
            .thenAnswer(deleteMessageBatchRequestCallback());

        // Replace the AmazonSQSClient with a mock alternative.
        sourceCounter = new SQSSourceCounter("test");
        source = new SQSSource()
            .withAmazonSQSClient(client)
            .withSourceCounter(sourceCounter);
        source.setName("test");
    }

    @After
    public void tearDown() {
        exception = ExpectedException.none();
    }

    private Answer<List<Message>> receiveMessageResultCallback() {

        // Callback routine to simulate the response from the call to Amazon
        // SQS to receive messages utilizing a batch request.
        return new Answer<List<Message>>() {
            @Override
            public synchronized List<Message>
            answer(InvocationOnMock invocation) throws Throwable {

                // Generate payload in batches.
                List<Message> messages = new ArrayList<Message>();
                while (numSentBatchRequests.get() < MAX_SENT_BATCH_REQUESTS) {
                    for (int i = 0; i < MAX_SENT_BATCH_SIZE; i++) {
                        Map<String, String> attributes =
                            new HashMap<String, String>();
                        attributes.put("SentTimestamp", "0");
                        attributes.put("ApproximateReceiveCount", "0");
                        messages.add(new Message()
                            .withBody("Body")
                            .withMD5OfBody("MD5OfBody")
                            .withMessageId("MessageId")
                            .withReceiptHandle("ReceiptHandle")
                            .withAttributes(attributes));
                        numSentMessages.incrementAndGet();

                    }
                    numSentBatchRequests.incrementAndGet();
                }
                return messages;
            }
        };
    }

    private Answer<DeleteMessageBatchResult>
    deleteMessageBatchRequestCallback() {

        // Callback routine to simulate the response from the call to Amazon
        // SQS to delete messages utilizing a batch request. Here we assume all
        // results are successful.
        return new Answer<DeleteMessageBatchResult>() {
            @Override
            public DeleteMessageBatchResult answer(InvocationOnMock invocation)
                throws Throwable {
                List<DeleteMessageBatchResultEntry> successful =
                    new ArrayList<DeleteMessageBatchResultEntry>();
                List<BatchResultErrorEntry> failed =
                    new ArrayList<BatchResultErrorEntry>();

                // Prepare the response and update internal counters to
                // indicate how many messages have been deleted.
                Object[] object = invocation.getArguments();
                DeleteMessageBatchRequest req =
                    (DeleteMessageBatchRequest) object[0];
                for (DeleteMessageBatchRequestEntry entry : req.getEntries()) {
                    successful.add(new DeleteMessageBatchResultEntry()
                        .withId(entry.getId()));
                    numDeletedMessages.incrementAndGet();
                }
                return new DeleteMessageBatchResult()
                    .withSuccessful(successful)
                    .withFailed(failed);
            }
        };
    }

    @Test
    public void testContextMandatoryPreconditions() {

        // Verify that an empty context throws an IllegalArgumentException
        // with an exception indicating that the mandatory option 'url' is not
        // defined.
        Context context = new Context();
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString(
            ConfigurationConstants.CONFIG_QUEUE_URL));
        Configurables.configure(source, context);
    }

    @Test
    public void testContextPositivePreconditions() {

        // List of context configuration options those arguments should always
        // be positive.
        ArrayList<String> options = new ArrayList<String>();
        options.add(ConfigurationConstants.CONFIG_RECV_BATCH_SIZE);
        options.add(ConfigurationConstants.CONFIG_RECV_TIMEOUT);
        options.add(ConfigurationConstants.CONFIG_RECV_VISTIMEOUT);
        options.add(ConfigurationConstants.CONFIG_BATCH_SIZE);
        options.add(ConfigurationConstants.CONFIG_NB_CONSUMER_THREADS);
        options.add(ConfigurationConstants.CONFIG_FLUSH_INTERVAL);

        // Loop over each option, initialize the mandatory context arguments
        // and verify that an IllegalArgumentException is thrown.
        int exceptions = 0;
        for (String option : options) {
            try {
                Context context = new Context();
                context.put(ConfigurationConstants.CONFIG_QUEUE_URL, "test");
                context.put(option, "0");
                Configurables.configure(source, context);
            } catch (IllegalArgumentException e) {
                exceptions++;
            }
        }
        assertEquals(exceptions, options.size());
    }

    @Test(timeout = 60000)
    public void testProcessing() throws InterruptedException {

        // Simple test to confirm the processing functionality of the SQS
        // Batch Consumer threads. Here we check that messages can be received,
        // processed and then deleted from SQS correctly.
        Channel channel = new MemoryChannel();
        Context context = new Context();
        context.put("capacity", "10000");
        context.put("transactionCapacity", "10000");
        Configurables.configure(channel, context);

        // Initialize SQS source.
        context = new Context();
        context.put(ConfigurationConstants.CONFIG_QUEUE_URL, "test");
        context.put(ConfigurationConstants.CONFIG_RECV_BATCH_SIZE, "10");
        context.put(ConfigurationConstants.CONFIG_BATCH_SIZE, "1000");

        // Force faster flushing operations.
        context.put(ConfigurationConstants.CONFIG_FLUSH_INTERVAL, "10");
        context.put(ConfigurationConstants.CONFIG_MAX_BACKOFF_SLEEP, "5");
        Configurables.configure(source, context);

        // Associate the channel to the source.
        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(Lists.newArrayList(channel));
        source.setChannelProcessor(new ChannelProcessor(rcs));
        source.start();

        // Wait for all messages to make it through the channel.
        long channelMessages = 0L;
        Transaction txn = rcs.getAllChannels().get(0).getTransaction();
        txn.begin();
        while (channelMessages < MAX_SENT_BATCH_SIZE * MAX_SENT_BATCH_REQUESTS) {
            if (channel.take() != null) {
                channelMessages++;
            } else {
                Thread.sleep(1000);
            }
        }
        txn.commit();
        txn.close();
        source.stop();

        // Verify all counters are correct.
        assertEquals("Not all messages deleted from SQS",
            numSentMessages.get(), numDeletedMessages.get());
        assertEquals("Not enough messages received",
            sourceCounter.getEventReceivedCount(), numSentMessages.get());
        assertEquals("Channel did not get all messages",
            channelMessages, numSentMessages.get());
    }
}
