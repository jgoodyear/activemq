/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.policy;

import org.apache.activemq.broker.QueueSubscriptionTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.region.policy.PolicyMap;

public class RoundRobinDispatchPolicyTest extends QueueSubscriptionTest {

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();

        PolicyEntry policy = new PolicyEntry();
        policy.setDispatchPolicy(new RoundRobinDispatchPolicy());

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);

        return broker;
    }

    public void testOneProducerTwoConsumersSmallMessagesOnePrefetch() throws Exception {
        super.testOneProducerTwoConsumersSmallMessagesOnePrefetch();

        // Ensure that each consumer should have received at least one message
        // We cannot guarantee that messages will be equally divided, since prefetch is one
        assertEachConsumerReceivedAtLeastXMessages(1);
    }

    public void testOneProducerTwoConsumersSmallMessagesLargePrefetch() throws Exception {
        super.testOneProducerTwoConsumersSmallMessagesLargePrefetch();
        assertMessagesDividedAmongConsumers();
    }

    public void testOneProducerTwoConsumersLargeMessagesOnePrefetch() throws Exception {
        super.testOneProducerTwoConsumersLargeMessagesOnePrefetch();

        // Ensure that each consumer should have received at least one message
        // We cannot guarantee that messages will be equally divided, since prefetch is one
        assertEachConsumerReceivedAtLeastXMessages(1);
    }

    public void testOneProducerTwoConsumersLargeMessagesLargePrefetch() throws Exception {
        super.testOneProducerTwoConsumersLargeMessagesLargePrefetch();
        assertMessagesDividedAmongConsumers();
    }

    public void testOneProducerManyConsumersFewMessages() throws Exception {
        super.testOneProducerManyConsumersFewMessages();

        // Since there are more consumers, each consumer should have received at most one message only
        assertMessagesDividedAmongConsumers();
    }

    public void testOneProducerManyConsumersManyMessages() throws Exception {
        super.testOneProducerManyConsumersManyMessages();
        assertMessagesDividedAmongConsumers();
    }

    public void testManyProducersManyConsumers() throws Exception {
        super.testManyProducersManyConsumers();
        assertMessagesDividedAmongConsumers();
    }

    public void assertMessagesDividedAmongConsumers() {
        assertEachConsumerReceivedAtLeastXMessages((messageCount * producerCount) / consumerCount);
        assertEachConsumerReceivedAtMostXMessages(((messageCount * producerCount) / consumerCount) + 1);
    }
}
