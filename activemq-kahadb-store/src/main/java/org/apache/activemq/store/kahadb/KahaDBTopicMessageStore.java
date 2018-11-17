/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadb;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.activemq.store.kahadb.data.KahaLocation;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KahaDBTopicMessageStore extends KahaDBMessageStore implements TopicMessageStore {
    private final AtomicInteger subscriptionCount = new AtomicInteger();
    protected final MessageStoreSubscriptionStatistics messageStoreSubStats;
    private final KahaDBStore kahaDBStore;

    public KahaDBTopicMessageStore(KahaDBStore kahaDBStore, ActiveMQTopic destination) throws IOException {
        super(kahaDBStore, destination);
        this.kahaDBStore = kahaDBStore;
        this.subscriptionCount.set(getAllSubscriptions().length);
        this.messageStoreSubStats = new MessageStoreSubscriptionStatistics(kahaDBStore.isEnableSubscriptionStatistics());
        if (kahaDBStore.isConcurrentStoreAndDispatchTopics()) {
            kahaDBStore.asyncTopicMaps.add(asyncTaskMap);
        }
    }

    @Override
    protected void recoverMessageStoreStatistics() throws IOException {
        super.recoverMessageStoreStatistics();
        this.recoverMessageStoreSubMetrics();
    }

    @Override
    public ListenableFuture<Object> asyncAddTopicMessage(final ConnectionContext context, final Message message)
            throws IOException {
        if (kahaDBStore.isConcurrentStoreAndDispatchTopics()) {
            message.beforeMarshall(kahaDBStore.wireFormat);
            StoreTopicTask result = new StoreTopicTask(kahaDBStore,this, context, message, subscriptionCount.get());
            result.aquireLocks();
            kahaDBStore.addTopicTask(this, result);
            return result.getFuture();
        } else {
            return super.asyncAddTopicMessage(context, message);
        }
    }

    @Override
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                            MessageId messageId, MessageAck ack) throws IOException {
        String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName).toString();
        if (kahaDBStore.isConcurrentStoreAndDispatchTopics()) {
            KahaDBStore.AsyncJobKey key = new KahaDBStore.AsyncJobKey(messageId, getDestination());
            StoreTopicTask task = null;
            synchronized (asyncTaskMap) {
                task = (StoreTopicTask) asyncTaskMap.get(key);
            }
            if (task != null) {
                if (task.addSubscriptionKey(subscriptionKey)) {
                    kahaDBStore.removeTopicTask(this, messageId);
                    if (task.cancel()) {
                        synchronized (asyncTaskMap) {
                            asyncTaskMap.remove(key);
                        }
                    }
                }
            } else {
                doAcknowledge(context, subscriptionKey, messageId, ack);
            }
        } else {
            doAcknowledge(context, subscriptionKey, messageId, ack);
        }
    }

    protected void doAcknowledge(ConnectionContext context, String subscriptionKey, MessageId messageId, MessageAck ack)
            throws IOException {
        KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
        command.setDestination(dest);
        command.setSubscriptionKey(subscriptionKey);
        command.setMessageId(messageId.toProducerKey());
        command.setTransactionInfo(ack != null ? TransactionIdConversion.convert(kahaDBStore.getTransactionIdTransformer().transform(ack.getTransactionId())) : null);
        if (ack != null && ack.isUnmatchedAck()) {
            command.setAck(MessageDatabase.UNMATCHED);
        } else {
            org.apache.activemq.util.ByteSequence packet = kahaDBStore.wireFormat.marshal(ack);
            command.setAck(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
        }
        kahaDBStore.store(command, false, null, null);
    }

    @Override
    public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        String subscriptionKey = kahaDBStore.subscriptionKey(subscriptionInfo.getClientId(), subscriptionInfo
                .getSubscriptionName());
        KahaSubscriptionCommand command = new KahaSubscriptionCommand();
        command.setDestination(dest);
        command.setSubscriptionKey(subscriptionKey.toString());
        command.setRetroactive(retroactive);
        org.apache.activemq.util.ByteSequence packet = kahaDBStore.wireFormat.marshal(subscriptionInfo);
        command.setSubscriptionInfo(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
        kahaDBStore.store(command, kahaDBStore.isEnableJournalDiskSyncs() && true, null, null);
        this.subscriptionCount.incrementAndGet();
    }

    @Override
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        KahaSubscriptionCommand command = new KahaSubscriptionCommand();
        command.setDestination(dest);
        command.setSubscriptionKey(kahaDBStore.subscriptionKey(clientId, subscriptionName).toString());
        kahaDBStore.store(command, kahaDBStore.isEnableJournalDiskSyncs() && true, null, null);
        this.subscriptionCount.decrementAndGet();
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {

        final ArrayList<SubscriptionInfo> subscriptions = new ArrayList<SubscriptionInfo>();
        kahaDBStore.indexLock.writeLock().lock();
        try {
            kahaDBStore.pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    for (Iterator<Map.Entry<String, KahaSubscriptionCommand>> iterator = sd.subscriptions.iterator(tx); iterator
                            .hasNext();) {
                        Map.Entry<String, KahaSubscriptionCommand> entry = iterator.next();
                        SubscriptionInfo info = (SubscriptionInfo) kahaDBStore.wireFormat.unmarshal(new DataInputStream(entry
                                .getValue().getSubscriptionInfo().newInput()));
                        subscriptions.add(info);

                    }
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }

        SubscriptionInfo[] rc = new SubscriptionInfo[subscriptions.size()];
        subscriptions.toArray(rc);
        return rc;
    }

    @Override
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        final String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName);
        kahaDBStore.indexLock.writeLock().lock();
        try {
            return kahaDBStore.pageFile.tx().execute(new Transaction.CallableClosure<SubscriptionInfo, IOException>() {
                @Override
                public SubscriptionInfo execute(Transaction tx) throws IOException {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    KahaSubscriptionCommand command = sd.subscriptions.get(tx, subscriptionKey);
                    if (command == null) {
                        return null;
                    }
                    return (SubscriptionInfo) kahaDBStore.wireFormat.unmarshal(new DataInputStream(command
                            .getSubscriptionInfo().newInput()));
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    @Override
    public int getMessageCount(String clientId, String subscriptionName) throws IOException {
        final String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName);

        if (kahaDBStore.isEnableSubscriptionStatistics()) {
            return (int)this.messageStoreSubStats.getMessageCount(subscriptionKey).getCount();
        } else {

            kahaDBStore.indexLock.writeLock().lock();
            try {
                return kahaDBStore.pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>() {
                    @Override
                    public Integer execute(Transaction tx) throws IOException {
                        MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                        MessageDatabase.LastAck cursorPos = kahaDBStore.getLastAck(tx, sd, subscriptionKey);
                        if (cursorPos == null) {
                            // The subscription might not exist.
                            return 0;
                        }

                        return (int) kahaDBStore.getStoredMessageCount(tx, sd, subscriptionKey);
                    }
                });
            } finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        }
    }


    @Override
    public long getMessageSize(String clientId, String subscriptionName) throws IOException {
        final String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName);
        if (kahaDBStore.isEnableSubscriptionStatistics()) {
            return this.messageStoreSubStats.getMessageSize(subscriptionKey).getTotalSize();
        } else {
            kahaDBStore.indexLock.writeLock().lock();
            try {
                return kahaDBStore.pageFile.tx().execute(new Transaction.CallableClosure<Long, IOException>() {
                    @Override
                    public Long execute(Transaction tx) throws IOException {
                        MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                        MessageDatabase.LastAck cursorPos = kahaDBStore.getLastAck(tx, sd, subscriptionKey);
                        if (cursorPos == null) {
                            // The subscription might not exist.
                            return 0l;
                        }

                        return kahaDBStore.getStoredMessageSize(tx, sd, subscriptionKey);
                    }
                });
            } finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        }
    }

    protected void recoverMessageStoreSubMetrics() throws IOException {
        if (kahaDBStore.isEnableSubscriptionStatistics()) {

            final MessageStoreSubscriptionStatistics statistics = getMessageStoreSubStatistics();
            kahaDBStore.indexLock.writeLock().lock();
            try {
                kahaDBStore.pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                        for (Iterator<Map.Entry<String, KahaSubscriptionCommand>> iterator = sd.subscriptions
                                .iterator(tx); iterator.hasNext();) {
                            Map.Entry<String, KahaSubscriptionCommand> entry = iterator.next();

                            String subscriptionKey = entry.getKey();
                            MessageDatabase.LastAck cursorPos = kahaDBStore.getLastAck(tx, sd, subscriptionKey);
                            if (cursorPos != null) {
                                long size =kahaDBStore. getStoredMessageSize(tx, sd, subscriptionKey);
                                statistics.getMessageCount(subscriptionKey)
                                        .setCount(kahaDBStore.getStoredMessageCount(tx, sd, subscriptionKey));
                                statistics.getMessageSize(subscriptionKey).addSize(size > 0 ? size : 0);
                            }
                        }
                    }
                });
            } finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener)
            throws Exception {
        final String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName);
        @SuppressWarnings("unused")
        final SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
        kahaDBStore.indexLock.writeLock().lock();
        try {
            kahaDBStore.pageFile.tx().execute(new Transaction.Closure<Exception>() {
                @Override
                public void execute(Transaction tx) throws Exception {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    MessageDatabase.LastAck cursorPos =kahaDBStore. getLastAck(tx, sd, subscriptionKey);
                    sd.orderIndex.setBatch(tx, cursorPos);
                    recoverRolledBackAcks(sd, tx, Integer.MAX_VALUE, listener);
                    for (Iterator<Map.Entry<Long, MessageDatabase.MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator
                            .hasNext();) {
                        Map.Entry<Long, MessageDatabase.MessageKeys> entry = iterator.next();
                        if (ackedAndPrepared.contains(entry.getValue().messageId)) {
                            continue;
                        }
                        listener.recoverMessage(kahaDBStore.loadMessage(entry.getValue().location));
                    }
                    sd.orderIndex.resetCursorPosition();
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    @Override
    public void recoverNextMessages(String clientId, String subscriptionName, final int maxReturned,
                                    final MessageRecoveryListener listener) throws Exception {
        final String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName);
        @SuppressWarnings("unused")
        final SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
        kahaDBStore.indexLock.writeLock().lock();
        try {
            kahaDBStore.pageFile.tx().execute(new Transaction.Closure<Exception>() {
                @Override
                public void execute(Transaction tx) throws Exception {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    sd.orderIndex.resetCursorPosition();
                    MessageDatabase.MessageOrderCursor moc = sd.subscriptionCursors.get(subscriptionKey);
                    if (moc == null) {
                        MessageDatabase.LastAck pos = kahaDBStore.getLastAck(tx, sd, subscriptionKey);
                        if (pos == null) {
                            // sub deleted
                            return;
                        }
                        sd.orderIndex.setBatch(tx, pos);
                        moc = sd.orderIndex.cursor;
                    } else {
                        sd.orderIndex.cursor.sync(moc);
                    }

                    Map.Entry<Long, MessageDatabase.MessageKeys> entry = null;
                    int counter = recoverRolledBackAcks(sd, tx, maxReturned, listener);
                    for (Iterator<Map.Entry<Long, MessageDatabase.MessageKeys>> iterator = sd.orderIndex.iterator(tx, moc); iterator
                            .hasNext();) {
                        entry = iterator.next();
                        if (ackedAndPrepared.contains(entry.getValue().messageId)) {
                            continue;
                        }
                        if (listener.recoverMessage(kahaDBStore.loadMessage(entry.getValue().location))) {
                            counter++;
                        }
                        if (counter >= maxReturned || listener.hasSpace() == false) {
                            break;
                        }
                    }
                    sd.orderIndex.stoppedIterating();
                    if (entry != null) {
                        MessageDatabase.MessageOrderCursor copy = sd.orderIndex.cursor.copy();
                        sd.subscriptionCursors.put(subscriptionKey, copy);
                    }
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        try {
            final String subscriptionKey = kahaDBStore.subscriptionKey(clientId, subscriptionName);
            kahaDBStore.indexLock.writeLock().lock();
            try {
                kahaDBStore.pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                        sd.subscriptionCursors.remove(subscriptionKey);
                    }
                });
            }finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
        return messageStoreSubStats;
    }
}