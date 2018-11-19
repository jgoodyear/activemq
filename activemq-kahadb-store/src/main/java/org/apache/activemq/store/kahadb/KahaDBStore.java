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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.NoLocalSubscriptionAware;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.activemq.store.kahadb.data.KahaLocation;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBStore extends MessageDatabase implements PersistenceAdapter, NoLocalSubscriptionAware {
    static final Logger LOG = LoggerFactory.getLogger(KahaDBStore.class);
    private static final int MAX_ASYNC_JOBS = BaseDestination.MAX_AUDIT_DEPTH;

    public static final String PROPERTY_CANCELED_TASK_MOD_METRIC = "org.apache.activemq.store.kahadb.CANCELED_TASK_MOD_METRIC";
    public static final int cancelledTaskModMetric = Integer.parseInt(System.getProperty(
            PROPERTY_CANCELED_TASK_MOD_METRIC, "0"), 10);
    public static final String PROPERTY_ASYNC_EXECUTOR_MAX_THREADS = "org.apache.activemq.store.kahadb.ASYNC_EXECUTOR_MAX_THREADS";
    private static final int asyncExecutorMaxThreads = Integer.parseInt(System.getProperty(
            PROPERTY_ASYNC_EXECUTOR_MAX_THREADS, "1"), 10);;

    protected ExecutorService queueExecutor;
    protected ExecutorService topicExecutor;
    protected final List<Map<AsyncJobKey, StoreTask>> asyncQueueMaps = new LinkedList<Map<AsyncJobKey, StoreTask>>();
    protected final List<Map<AsyncJobKey, StoreTask>> asyncTopicMaps = new LinkedList<Map<AsyncJobKey, StoreTask>>();
    final WireFormat wireFormat = new OpenWireFormat();
    private SystemUsage usageManager;
    private LinkedBlockingQueue<Runnable> asyncQueueJobQueue;
    private LinkedBlockingQueue<Runnable> asyncTopicJobQueue;
    public Semaphore globalQueueSemaphore;
    public Semaphore globalTopicSemaphore;
    private boolean concurrentStoreAndDispatchQueues = true;
    // when true, message order may be compromised when cache is exhausted if store is out
    // or order w.r.t cache
    private boolean concurrentStoreAndDispatchTopics = false;
    private final boolean concurrentStoreAndDispatchTransactions = false;
    private int maxAsyncJobs = MAX_ASYNC_JOBS;
    private final KahaDBTransactionStore transactionStore;
    private TransactionIdTransformer transactionIdTransformer;

    public KahaDBStore() {
        this.transactionStore = new KahaDBTransactionStore(this);
        this.transactionIdTransformer = new TransactionIdTransformer() {
            @Override
            public TransactionId transform(TransactionId txid) {
                return txid;
            }
        };
    }

    @Override
    public String toString() {
        return "KahaDB:[" + directory.getAbsolutePath() + "]";
    }

    @Override
    public void setBrokerName(String brokerName) {
    }

    @Override
    public void setUsageManager(SystemUsage usageManager) {
        this.usageManager = usageManager;
    }

    public SystemUsage getUsageManager() {
        return this.usageManager;
    }

    /**
     * @return the concurrentStoreAndDispatch
     */
    public boolean isConcurrentStoreAndDispatchQueues() {
        return this.concurrentStoreAndDispatchQueues;
    }

    /**
     * @param concurrentStoreAndDispatch
     *            the concurrentStoreAndDispatch to set
     */
    public void setConcurrentStoreAndDispatchQueues(boolean concurrentStoreAndDispatch) {
        this.concurrentStoreAndDispatchQueues = concurrentStoreAndDispatch;
    }

    /**
     * @return the concurrentStoreAndDispatch
     */
    public boolean isConcurrentStoreAndDispatchTopics() {
        return this.concurrentStoreAndDispatchTopics;
    }

    /**
     * @param concurrentStoreAndDispatch
     *            the concurrentStoreAndDispatch to set
     */
    public void setConcurrentStoreAndDispatchTopics(boolean concurrentStoreAndDispatch) {
        this.concurrentStoreAndDispatchTopics = concurrentStoreAndDispatch;
    }

    public boolean isConcurrentStoreAndDispatchTransactions() {
        return this.concurrentStoreAndDispatchTransactions;
    }

    /**
     * @return the maxAsyncJobs
     */
    public int getMaxAsyncJobs() {
        return this.maxAsyncJobs;
    }

    /**
     * @param maxAsyncJobs
     *            the maxAsyncJobs to set
     */
    public void setMaxAsyncJobs(int maxAsyncJobs) {
        this.maxAsyncJobs = maxAsyncJobs;
    }


    @Override
    protected void configureMetadata() {
        if (brokerService != null) {
            metadata.openwireVersion = brokerService.getStoreOpenWireVersion();
            wireFormat.setVersion(metadata.openwireVersion);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Store OpenWire version configured as: {}", metadata.openwireVersion);
            }

        }
    }

    @Override
    public void doStart() throws Exception {
        //configure the metadata before start, right now
        //this is just the open wire version
        configureMetadata();

        super.doStart();

        if (brokerService != null) {
            // In case the recovered store used a different OpenWire version log a warning
            // to assist in determining why journal reads fail.
            if (metadata.openwireVersion != brokerService.getStoreOpenWireVersion()) {
                LOG.warn("Existing Store uses a different OpenWire version[{}] " +
                         "than the version configured[{}] reverting to the version " +
                         "used by this store, some newer broker features may not work" +
                         "as expected.",
                         metadata.openwireVersion, brokerService.getStoreOpenWireVersion());

                // Update the broker service instance to the actual version in use.
                wireFormat.setVersion(metadata.openwireVersion);
                brokerService.setStoreOpenWireVersion(metadata.openwireVersion);
            }
        }

        this.globalQueueSemaphore = new Semaphore(getMaxAsyncJobs());
        this.globalTopicSemaphore = new Semaphore(getMaxAsyncJobs());
        this.asyncQueueJobQueue = new LinkedBlockingQueue<Runnable>(getMaxAsyncJobs());
        this.asyncTopicJobQueue = new LinkedBlockingQueue<Runnable>(getMaxAsyncJobs());
        this.queueExecutor = new StoreTaskExecutor(1, asyncExecutorMaxThreads, 0L, TimeUnit.MILLISECONDS,
            asyncQueueJobQueue, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ConcurrentQueueStoreAndDispatch");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        this.topicExecutor = new StoreTaskExecutor(1, asyncExecutorMaxThreads, 0L, TimeUnit.MILLISECONDS,
            asyncTopicJobQueue, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ConcurrentTopicStoreAndDispatch");
                    thread.setDaemon(true);
                    return thread;
                }
            });
    }

    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        // drain down async jobs
        LOG.info("Stopping async queue tasks");
        if (this.globalQueueSemaphore != null) {
            this.globalQueueSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS);
        }
        synchronized (this.asyncQueueMaps) {
            for (Map<AsyncJobKey, StoreTask> m : asyncQueueMaps) {
                synchronized (m) {
                    for (StoreTask task : m.values()) {
                        task.cancel();
                    }
                }
            }
            this.asyncQueueMaps.clear();
        }
        LOG.info("Stopping async topic tasks");
        if (this.globalTopicSemaphore != null) {
            this.globalTopicSemaphore.tryAcquire(this.maxAsyncJobs, 60, TimeUnit.SECONDS);
        }
        synchronized (this.asyncTopicMaps) {
            for (Map<AsyncJobKey, StoreTask> m : asyncTopicMaps) {
                synchronized (m) {
                    for (StoreTask task : m.values()) {
                        task.cancel();
                    }
                }
            }
            this.asyncTopicMaps.clear();
        }
        if (this.globalQueueSemaphore != null) {
            this.globalQueueSemaphore.drainPermits();
        }
        if (this.globalTopicSemaphore != null) {
            this.globalTopicSemaphore.drainPermits();
        }
        if (this.queueExecutor != null) {
            ThreadPoolUtils.shutdownNow(queueExecutor);
            queueExecutor = null;
        }
        if (this.topicExecutor != null) {
            ThreadPoolUtils.shutdownNow(topicExecutor);
            topicExecutor = null;
        }
        LOG.info("Stopped KahaDB");
        super.doStop(stopper);
    }

    protected Location findMessageLocation(final String key, final KahaDestination destination) throws IOException {
        return pageFile.tx().execute(new Transaction.CallableClosure<Location, IOException>() {
            @Override
            public Location execute(Transaction tx) throws IOException {
                StoredDestination sd = getStoredDestination(destination, tx);
                Long sequence = sd.messageIdIndex.get(tx, key);
                if (sequence == null) {
                    return null;
                }
                return sd.orderIndex.get(tx, sequence).location;
            }
        });
    }

    protected StoreQueueTask removeQueueTask(KahaDBMessageStore store, MessageId id) {
        StoreQueueTask task = null;
        synchronized (store.asyncTaskMap) {
            task = (StoreQueueTask) store.asyncTaskMap.remove(new AsyncJobKey(id, store.getDestination()));
        }
        return task;
    }

    // with asyncTaskMap locked
    protected void addQueueTask(KahaDBMessageStore store, StoreQueueTask task) throws IOException {
        store.asyncTaskMap.put(new AsyncJobKey(task.getMessage().getMessageId(), store.getDestination()), task);
        this.queueExecutor.execute(task);
    }

    protected StoreTopicTask removeTopicTask(KahaDBTopicMessageStore store, MessageId id) {
        StoreTopicTask task = null;
        synchronized (store.asyncTaskMap) {
            task = (StoreTopicTask) store.asyncTaskMap.remove(new AsyncJobKey(id, store.getDestination()));
        }
        return task;
    }

    protected void addTopicTask(KahaDBTopicMessageStore store, StoreTopicTask task) throws IOException {
        synchronized (store.asyncTaskMap) {
            store.asyncTaskMap.put(new AsyncJobKey(task.getMessage().getMessageId(), store.getDestination()), task);
        }
        this.topicExecutor.execute(task);
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        return this.transactionStore;
    }

    public boolean getForceRecoverIndex() {
        return this.forceRecoverIndex;
    }

    public void setForceRecoverIndex(boolean forceRecoverIndex) {
        this.forceRecoverIndex = forceRecoverIndex;
    }

    public void forgetRecoveredAcks(ArrayList<MessageAck> preparedAcks, boolean isRollback) throws IOException {
        if (preparedAcks != null) {
            Map<ActiveMQDestination, KahaDBMessageStore> stores = new HashMap<>();
            for (MessageAck ack : preparedAcks) {
                stores.put(ack.getDestination(), findMatchingStore(ack.getDestination()));
            }
            ArrayList<MessageAck> perStoreAcks = new ArrayList<>();
            for (Entry<ActiveMQDestination, KahaDBMessageStore> entry : stores.entrySet()) {
                for (MessageAck ack : preparedAcks) {
                    if (entry.getKey().equals(ack.getDestination())) {
                        perStoreAcks.add(ack);
                    }
                }
                entry.getValue().forgetRecoveredAcks(perStoreAcks, isRollback);
                perStoreAcks.clear();
            }
        }
    }

    public void trackRecoveredAcks(ArrayList<MessageAck> preparedAcks) throws IOException {
        Map<ActiveMQDestination, KahaDBMessageStore> stores = new HashMap<>();
        for (MessageAck ack : preparedAcks) {
            stores.put(ack.getDestination(), findMatchingStore(ack.getDestination()));
        }
        ArrayList<MessageAck> perStoreAcks = new ArrayList<>();
        for (Entry<ActiveMQDestination, KahaDBMessageStore> entry : stores.entrySet()) {
            for (MessageAck ack : preparedAcks) {
                if (entry.getKey().equals(ack.getDestination())) {
                    perStoreAcks.add(ack);
                }
            }
            entry.getValue().trackRecoveredAcks(perStoreAcks);
            perStoreAcks.clear();
        }
    }

    private KahaDBMessageStore findMatchingStore(ActiveMQDestination activeMQDestination) throws IOException {
        ProxyMessageStore store = (ProxyMessageStore) storeCache.get(convert(activeMQDestination));
        if (store == null) {
            if (activeMQDestination.isQueue()) {
                store = (ProxyMessageStore) createQueueMessageStore((ActiveMQQueue) activeMQDestination);
            } else {
                store = (ProxyMessageStore) createTopicMessageStore((ActiveMQTopic) activeMQDestination);
            }
        }
        return (KahaDBMessageStore) store.getDelegate();
    }





    String subscriptionKey(String clientId, String subscriptionName) {
        return clientId + ":" + subscriptionName;
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        String key = key(convert(destination));
        MessageStore store = storeCache.get(key(convert(destination)));
        if (store == null) {
            final MessageStore queueStore = this.transactionStore.proxy(new KahaDBMessageStore(this, destination));
            store = storeCache.putIfAbsent(key, queueStore);
            if (store == null) {
                store = queueStore;
            }
        }

        return store;
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        String key = key(convert(destination));
        MessageStore store = storeCache.get(key(convert(destination)));
        if (store == null) {
            final TopicMessageStore topicStore = this.transactionStore.proxy(new KahaDBTopicMessageStore(this, destination));
            store = storeCache.putIfAbsent(key, topicStore);
            if (store == null) {
                store = topicStore;
            }
        }

        return (TopicMessageStore) store;
    }

    /**
     * Cleanup method to remove any state associated with the given destination.
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination
     *            Destination to forget
     */
    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination
     *            Destination to forget
     */
    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    @Override
    public void deleteAllMessages() throws IOException {
        deleteAllMessages = true;
    }

    @Override
    public Set<ActiveMQDestination> getDestinations() {
        try {
            final HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
            indexLock.writeLock().lock();
            try {
                pageFile.tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator
                                .hasNext();) {
                            Entry<String, StoredDestination> entry = iterator.next();
                            //Removing isEmpty topic check - see AMQ-5875
                            rc.add(convert(entry.getKey()));
                        }
                    }
                });
            }finally {
                indexLock.writeLock().unlock();
            }
            return rc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) {
        indexLock.writeLock().lock();
        try {
            return metadata.producerSequenceIdTracker.getLastSeqId(id);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        try {
            return journalSize.get() + getPageFile().getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    @Override
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }

    @Override
    public void checkpoint(boolean sync) throws IOException {
        super.checkpointCleanup(sync);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal helper methods.
    // /////////////////////////////////////////////////////////////////

    /**
     * @param location
     * @return
     * @throws IOException
     */
    Message loadMessage(Location location) throws IOException {
        try {
            JournalCommand<?> command = load(location);
            KahaAddMessageCommand addMessage = null;
            switch (command.type()) {
                case KAHA_UPDATE_MESSAGE_COMMAND:
                    addMessage = ((KahaUpdateMessageCommand) command).getMessage();
                    break;
                case KAHA_ADD_MESSAGE_COMMAND:
                    addMessage = (KahaAddMessageCommand) command;
                    break;
                default:
                    throw new IOException("Could not load journal record, unexpected command type: " + command.type() + " at location: " + location);
            }
            if (!addMessage.hasMessage()) {
                throw new IOException("Could not load journal record, null message content at location: " + location);
            }
            Message msg = (Message) wireFormat.unmarshal(new DataInputStream(addMessage.getMessage().newInput()));
            return msg;
        } catch (Throwable t) {
            IOException ioe = IOExceptionSupport.create("Unexpected error on journal read at: " + location , t);
            LOG.error("Failed to load message at: {}", location , ioe);
            brokerService.handleIOException(ioe);
            throw ioe;
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Internal conversion methods.
    // /////////////////////////////////////////////////////////////////

    KahaLocation convert(Location location) {
        KahaLocation rc = new KahaLocation();
        rc.setLogId(location.getDataFileId());
        rc.setOffset(location.getOffset());
        return rc;
    }

    KahaDestination convert(ActiveMQDestination dest) {
        KahaDestination rc = new KahaDestination();
        rc.setName(dest.getPhysicalName());
        switch (dest.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            rc.setType(DestinationType.QUEUE);
            return rc;
        case ActiveMQDestination.TOPIC_TYPE:
            rc.setType(DestinationType.TOPIC);
            return rc;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            rc.setType(DestinationType.TEMP_QUEUE);
            return rc;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            rc.setType(DestinationType.TEMP_TOPIC);
            return rc;
        default:
            return null;
        }
    }

    ActiveMQDestination convert(String dest) {
        int p = dest.indexOf(":");
        if (p < 0) {
            throw new IllegalArgumentException("Not in the valid destination format");
        }
        int type = Integer.parseInt(dest.substring(0, p));
        String name = dest.substring(p + 1);
        return convert(type, name);
    }

    private ActiveMQDestination convert(KahaDestination commandDestination) {
        return convert(commandDestination.getType().getNumber(), commandDestination.getName());
    }

    private ActiveMQDestination convert(int type, String name) {
        switch (KahaDestination.DestinationType.valueOf(type)) {
        case QUEUE:
            return new ActiveMQQueue(name);
        case TOPIC:
            return new ActiveMQTopic(name);
        case TEMP_QUEUE:
            return new ActiveMQTempQueue(name);
        case TEMP_TOPIC:
            return new ActiveMQTempTopic(name);
        default:
            throw new IllegalArgumentException("Not in the valid destination format");
        }
    }

    public TransactionIdTransformer getTransactionIdTransformer() {
        return transactionIdTransformer;
    }

    public void setTransactionIdTransformer(TransactionIdTransformer transactionIdTransformer) {
        this.transactionIdTransformer = transactionIdTransformer;
    }

    static class AsyncJobKey {
        MessageId id;
        ActiveMQDestination destination;

        AsyncJobKey(MessageId id, ActiveMQDestination destination) {
            this.id = id;
            this.destination = destination;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            return obj instanceof AsyncJobKey && id.equals(((AsyncJobKey) obj).id)
                    && destination.equals(((AsyncJobKey) obj).destination);
        }

        @Override
        public int hashCode() {
            return id.hashCode() + destination.hashCode();
        }

        @Override
        public String toString() {
            return destination.getPhysicalName() + "-" + id;
        }
    }

    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        return new JobSchedulerStoreImpl();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.NoLocalSubscriptionAware#isPersistNoLocal()
     */
    @Override
    public boolean isPersistNoLocal() {
        // Prior to v11 the broker did not store the noLocal value for durable subs.
        return brokerService.getStoreOpenWireVersion() >= 11;
    }
}
