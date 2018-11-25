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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStoreStatistics;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.usage.MemoryUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KahaDBMessageStore extends AbstractMessageStore {
    static final Logger LOG = LoggerFactory.getLogger(KahaDBMessageStore.class);
    protected final Map<KahaDBStore.AsyncJobKey, StoreTask> asyncTaskMap = new HashMap<KahaDBStore.AsyncJobKey, StoreTask>();
    protected KahaDestination dest;
    private final int maxAsyncJobs;
    private final Semaphore localDestinationSemaphore;
    private final KahaDBStore kahaDBStore;
    protected final Set<String> ackedAndPrepared = new HashSet<>();
    protected final Set<String> rolledBackAcks = new HashSet<>();


    public KahaDBMessageStore(KahaDBStore kahaDBStore, ActiveMQDestination destination) {
        super(destination);
        this.kahaDBStore = kahaDBStore;
        this.dest = kahaDBStore.convert(destination);
        this.maxAsyncJobs = kahaDBStore.getMaxAsyncJobs();
        this.localDestinationSemaphore = new Semaphore(this.maxAsyncJobs);
    }

    @Override
    public ActiveMQDestination getDestination() {
        return destination;
    }


    // messages that have prepared (pending) acks cannot be re-dispatched unless the outcome is rollback,
    // till then they are skipped by the store.
    // 'at most once' XA guarantee
    public void trackRecoveredAcks(ArrayList<MessageAck> acks) {
        kahaDBStore.indexLock.writeLock().lock();
        try {
            for (MessageAck ack : acks) {
                ackedAndPrepared.add(ack.getLastMessageId().toProducerKey());
            }
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    public void forgetRecoveredAcks(ArrayList<MessageAck> acks, boolean rollback) throws IOException {
        if (acks != null) {
            kahaDBStore.indexLock.writeLock().lock();
            try {
                for (MessageAck ack : acks) {
                    final String id = ack.getLastMessageId().toProducerKey();
                    ackedAndPrepared.remove(id);
                    if (rollback) {
                        rolledBackAcks.add(id);
                        kahaDBStore.incrementAndAddSizeToStoreStat(dest, 0);
                    }
                }
            } finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        }
    }

    @Override
    public ListenableFuture<Object> asyncAddQueueMessage(final ConnectionContext context, final Message message)
            throws IOException {
        if (kahaDBStore.isConcurrentStoreAndDispatchQueues()) {
            message.beforeMarshall(kahaDBStore.wireFormat);
            StoreQueueTask result = new StoreQueueTask(kahaDBStore,this, context, message,
                                                       new KahaDBMessageStoreTaskCompletionListener());
            ListenableFuture<Object> future = result.getFuture();
            message.getMessageId().setFutureOrSequenceLong(future);
            message.setRecievedByDFBridge(true); // flag message as concurrentStoreAndDispatch
            result.aquireLocks();
            synchronized (asyncTaskMap) {
                kahaDBStore.addQueueTask(this, result);
                if (indexListener != null) {
                    indexListener.onAdd(new IndexListener.MessageContext(context, message, null));
                }
            }
            return future;
        } else {
            return super.asyncAddQueueMessage(context, message);
        }
    }

    @Override
    public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
        if (kahaDBStore.isConcurrentStoreAndDispatchQueues()) {
            KahaDBStore.AsyncJobKey key = new KahaDBStore.AsyncJobKey(ack.getLastMessageId(), getDestination());
            StoreQueueTask task = null;
            synchronized (asyncTaskMap) {
                task = (StoreQueueTask) asyncTaskMap.get(key);
            }
            if (task != null) {
                if (ack.isInTransaction() || !task.cancel()) {
                    try {
                        task.future.get();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException(e.toString());
                    } catch (Exception ignored) {
                        LOG.debug("removeAsync: cannot cancel, waiting for add resulted in ex", ignored);
                    }
                    removeMessage(context, ack);
                } else {
                    kahaDBStore.indexLock.writeLock().lock();
                    try {
                        kahaDBStore.metadata.producerSequenceIdTracker.isDuplicate(ack.getLastMessageId());
                    } finally {
                        kahaDBStore.indexLock.writeLock().unlock();
                    }
                    synchronized (asyncTaskMap) {
                        asyncTaskMap.remove(key);
                    }
                }
            } else {
                removeMessage(context, ack);
            }
        } else {
            removeMessage(context, ack);
        }
    }

    @Override
    public void addMessage(final ConnectionContext context, final Message message) throws IOException {
        final KahaAddMessageCommand command = new KahaAddMessageCommand();
        command.setDestination(dest);
        command.setMessageId(message.getMessageId().toProducerKey());
        command.setTransactionInfo(TransactionIdConversion.convert(kahaDBStore.getTransactionIdTransformer().transform(message.getTransactionId())));
        command.setPriority(message.getPriority());
        command.setPrioritySupported(isPrioritizedMessages());
        org.apache.activemq.util.ByteSequence packet = kahaDBStore.wireFormat.marshal(message);
        command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
        kahaDBStore.store(command, kahaDBStore.isEnableJournalDiskSyncs() && message.isResponseRequired(), new MessageDatabase.IndexAware() {
            // sync add? (for async, future present from getFutureOrSequenceLong)
            Object possibleFuture = message.getMessageId().getFutureOrSequenceLong();

            @Override
            public void sequenceAssignedWithIndexLocked(final long sequence) {
                message.getMessageId().setFutureOrSequenceLong(sequence);
                if (indexListener != null) {
                    if (possibleFuture == null) {
                        kahaDBStore.trackPendingAdd(dest, sequence);
                        indexListener.onAdd(new IndexListener.MessageContext(context, message, new Runnable() {
                            @Override
                            public void run() {
                                kahaDBStore.trackPendingAddComplete(dest, sequence);
                            }
                        }));
                    }
                }
            }
        }, null);
    }

    @Override
    public void updateMessage(Message message) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("updating: " + message.getMessageId() + " with deliveryCount: " + message.getRedeliveryCounter());
        }
        KahaUpdateMessageCommand updateMessageCommand = new KahaUpdateMessageCommand();
        KahaAddMessageCommand command = new KahaAddMessageCommand();
        command.setDestination(dest);
        command.setMessageId(message.getMessageId().toProducerKey());
        command.setPriority(message.getPriority());
        command.setPrioritySupported(prioritizedMessages);
        org.apache.activemq.util.ByteSequence packet = kahaDBStore.wireFormat.marshal(message);
        command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
        updateMessageCommand.setMessage(command);
        kahaDBStore.store(updateMessageCommand, kahaDBStore.isEnableJournalDiskSyncs(), null, null);
    }

    @Override
    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
        command.setDestination(dest);
        command.setMessageId(ack.getLastMessageId().toProducerKey());
        command.setTransactionInfo(TransactionIdConversion.convert(kahaDBStore.getTransactionIdTransformer().transform(ack.getTransactionId())));

        org.apache.activemq.util.ByteSequence packet = kahaDBStore.wireFormat.marshal(ack);
        command.setAck(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
        kahaDBStore.store(command, kahaDBStore.isEnableJournalDiskSyncs() && ack.isResponseRequired(), null, null);
    }

    @Override
    public void removeAllMessages(ConnectionContext context) throws IOException {
        KahaRemoveDestinationCommand command = new KahaRemoveDestinationCommand();
        command.setDestination(dest);
        kahaDBStore.store(command, true, null, null);
    }

    @Override
    public Message getMessage(MessageId identity) throws IOException {
        final String key = identity.toProducerKey();

        // Hopefully one day the page file supports concurrent read
        // operations... but for now we must
        // externally synchronize...
        Location location;
        kahaDBStore.indexLock.writeLock().lock();
        try {
            location = kahaDBStore.findMessageLocation(key, dest);
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
        if (location == null) {
            return null;
        }

        return kahaDBStore.loadMessage(location);
    }

    @Override
    public boolean isEmpty() throws IOException {
        kahaDBStore.indexLock.writeLock().lock();
        try {
            return kahaDBStore.pageFile.tx().execute(new Transaction.CallableClosure<Boolean, IOException>() {
                @Override
                public Boolean execute(Transaction tx) throws IOException {
                    // Iterate through all index entries to get a count of
                    // messages in the destination.
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    return sd.locationIndex.isEmpty(tx);
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    @Override
    public void recover(final MessageRecoveryListener listener) throws Exception {
        // recovery may involve expiry which will modify
        kahaDBStore.indexLock.writeLock().lock();
        try {
            kahaDBStore.pageFile.tx().execute(new Transaction.Closure<Exception>() {
                @Override
                public void execute(Transaction tx) throws Exception {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    recoverRolledBackAcks(sd, tx, Integer.MAX_VALUE, listener);
                    sd.orderIndex.resetCursorPosition();
                    for (Iterator<Map.Entry<Long, MessageDatabase.MessageKeys>> iterator = sd.orderIndex.iterator(tx); listener.hasSpace() && iterator
                            .hasNext(); ) {
                        Map.Entry<Long, MessageDatabase.MessageKeys> entry = iterator.next();
                        if (ackedAndPrepared.contains(entry.getValue().messageId)) {
                            continue;
                        }
                        Message msg = kahaDBStore.loadMessage(entry.getValue().location);
                        listener.recoverMessage(msg);
                    }
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    @Override
    public void recoverNextMessages(final int maxReturned, final MessageRecoveryListener listener) throws Exception {
        kahaDBStore.indexLock.writeLock().lock();
        try {
            kahaDBStore.pageFile.tx().execute(new Transaction.Closure<Exception>() {
                @Override
                public void execute(Transaction tx) throws Exception {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    Map.Entry<Long, MessageDatabase.MessageKeys> entry = null;
                    int counter = recoverRolledBackAcks(sd, tx, maxReturned, listener);
                    for (Iterator<Map.Entry<Long, MessageDatabase.MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext(); ) {
                        entry = iterator.next();
                        if (ackedAndPrepared.contains(entry.getValue().messageId)) {
                            continue;
                        }
                        Message msg = kahaDBStore.loadMessage(entry.getValue().location);
                        msg.getMessageId().setFutureOrSequenceLong(entry.getKey());
                        listener.recoverMessage(msg);
                        counter++;
                        if (counter >= maxReturned) {
                            break;
                        }
                    }
                    sd.orderIndex.stoppedIterating();
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    protected int recoverRolledBackAcks(MessageDatabase.StoredDestination sd, Transaction tx, int maxReturned, MessageRecoveryListener listener) throws Exception {
        int counter = 0;
        String id;
        for (Iterator<String> iterator = rolledBackAcks.iterator(); iterator.hasNext(); ) {
            id = iterator.next();
            iterator.remove();
            Long sequence = sd.messageIdIndex.get(tx, id);
            if (sequence != null) {
                if (sd.orderIndex.alreadyDispatched(sequence)) {
                    listener.recoverMessage(kahaDBStore.loadMessage(sd.orderIndex.get(tx, sequence).location));
                    counter++;
                    if (counter >= maxReturned) {
                        break;
                    }
                } else {
                    LOG.info("rolledback ack message {} with seq {} will be picked up in future batch {}", id, sequence, sd.orderIndex.cursor);
                }
            } else {
                LOG.warn("Failed to locate rolled back ack message {} in {}", id, sd);
            }
        }
        return counter;
    }


    @Override
    public void resetBatching() {
        if (kahaDBStore.pageFile.isLoaded()) {
            kahaDBStore.indexLock.writeLock().lock();
            try {
                kahaDBStore.pageFile.tx().execute(new Transaction.Closure<Exception>() {
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        MessageDatabase.StoredDestination sd = kahaDBStore.getExistingStoredDestination(dest, tx);
                        if (sd != null) {
                            sd.orderIndex.resetCursorPosition();}
                    }
                });
            } catch (Exception e) {
                LOG.error("Failed to reset batching",e);
            } finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void setBatch(final MessageId identity) throws IOException {
        kahaDBStore.indexLock.writeLock().lock();
        try {
            kahaDBStore.pageFile.tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                    Long location = (Long) identity.getFutureOrSequenceLong();
                    Long pending = sd.orderIndex.minPendingAdd();
                    if (pending != null) {
                        location = Math.min(location, pending-1);
                    }
                    sd.orderIndex.setBatch(tx, location);
                }
            });
        } finally {
            kahaDBStore.indexLock.writeLock().unlock();
        }
    }

    @Override
    public void setMemoryUsage(MemoryUsage memoryUsage) {
    }
    @Override
    public void start() throws Exception {
        super.start();
    }
    @Override
    public void stop() throws Exception {
        super.stop();
    }

    protected void lockAsyncJobQueue() {
        try {
            if (!this.localDestinationSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS)) {
                throw new TimeoutException(this +" timeout waiting for localDestSem:" + this.localDestinationSemaphore);
            }
        } catch (Exception e) {
            LOG.error("Failed to lock async jobs for " + this.destination, e);
        }
    }

    protected void unlockAsyncJobQueue() {
        this.localDestinationSemaphore.release(this.maxAsyncJobs);
    }

    protected void acquireLocalAsyncLock() {
        try {
            this.localDestinationSemaphore.acquire();
        } catch (InterruptedException e) {
            LOG.error("Failed to aquire async lock for " + this.destination, e);
        }
    }

    protected void releaseLocalAsyncLock() {
        this.localDestinationSemaphore.release();
    }

    @Override
    public String toString(){
        return "permits:" + this.localDestinationSemaphore.availablePermits() + ",sd=" + kahaDBStore.storedDestinations.get(kahaDBStore.key(dest));
    }

    @Override
    protected void recoverMessageStoreStatistics() throws IOException {
        try {
            MessageStoreStatistics recoveredStatistics;
            lockAsyncJobQueue();
            kahaDBStore.indexLock.writeLock().lock();
            try {
                recoveredStatistics = kahaDBStore.pageFile.tx().execute(new Transaction.CallableClosure<MessageStoreStatistics, IOException>() {
                    @Override
                    public MessageStoreStatistics execute(Transaction tx) throws IOException {
                        MessageStoreStatistics statistics = new MessageStoreStatistics();

                        // Iterate through all index entries to get the size of each message
                        MessageDatabase.StoredDestination sd = kahaDBStore.getStoredDestination(dest, tx);
                        for (Iterator<Map.Entry<Location, Long>> iterator = sd.locationIndex.iterator(tx); iterator.hasNext();) {
                            int locationSize = iterator.next().getKey().getSize();
                            statistics.getMessageCount().increment();
                            statistics.getMessageSize().addSize(locationSize > 0 ? locationSize : 0);
                        }
                        return statistics;
                    }
                });
                recoveredStatistics.getMessageCount().subtract(ackedAndPrepared.size());
                getMessageStoreStatistics().getMessageCount().setCount(recoveredStatistics.getMessageCount().getCount());
                getMessageStoreStatistics().getMessageSize().setTotalSize(recoveredStatistics.getMessageSize().getTotalSize());
            } finally {
                kahaDBStore.indexLock.writeLock().unlock();
            }
        } finally {
            unlockAsyncJobQueue();
        }
    }

//========================================
// Internal Classes
//----------------------------------------

    protected class KahaDBMessageStoreTaskCompletionListener implements StoreTaskCompletionListener {

        public long doneTasks = 0;
        public long canceledTasks = 0;

        @Override
        public void taskCompleted() {
            this.doneTasks++;
        }

        @Override
        public void taskCanceled() {
            this.canceledTasks++;

            // Report the canceled task, if enabled
            if ((KahaDBStore.cancelledTaskModMetric > 0) &&
                (this.canceledTasks % kahaDBStore.getCancelledTaskModMetric() == 0)) {

                // TODO: shouldn't this use a logger?
                System.err.println(KahaDBMessageStore.this.dest.getName() + " cancelled: " +
                                   this.calcPercentage(this.canceledTasks, this.doneTasks));

                this.canceledTasks = 0;
                this.doneTasks = 0;
            }
        }

        private double calcPercentage(long amount, long total) {
            return ((double) amount / (double) total) * 100.0;
        }

    }
}
