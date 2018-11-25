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
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StoreTopicTask extends StoreQueueTask {

    private static final Logger LOG = LoggerFactory.getLogger(StoreTopicTask.class);
    private final int subscriptionCount;
    private final List<String> subscriptionKeys = new ArrayList<String>(1);
    protected final KahaDBStore kahaDBStore;
    private final KahaDBTopicMessageStore topicStore;

    private final StoreTaskCompletionListener storeTaskCompletionListener;

    public StoreTopicTask(KahaDBStore kahaDBStore, KahaDBTopicMessageStore store, ConnectionContext context, Message message,
                          int subscriptionCount, StoreTaskCompletionListener storeTaskCompletionListener) {
        super(kahaDBStore, store, context, message, storeTaskCompletionListener);
        this.kahaDBStore = kahaDBStore;
        this.topicStore = store;
        this.subscriptionCount = subscriptionCount;
        this.storeTaskCompletionListener = storeTaskCompletionListener;
    }

    @Override
    public void aquireLocks() {
        if (this.locked.compareAndSet(false, true)) {
            try {
                kahaDBStore.globalTopicSemaphore.acquire();
                store.acquireLocalAsyncLock();
                message.incrementReferenceCount();
            } catch (InterruptedException e) {
                LOG.warn("Failed to aquire lock", e);
            }
        }
    }

    @Override
    public void releaseLocks() {
        if (this.locked.compareAndSet(true, false)) {
            message.decrementReferenceCount();
            store.releaseLocalAsyncLock();
            kahaDBStore.globalTopicSemaphore.release();
        }
    }

    /**
     * add a key
     *
<<<<<<< HEAD
     * @param key to add to subscription keys
=======
     * @param key
>>>>>>> bb21dd021799ce0492021997cf5203cb3dbe3678
     * @return true if all acknowledgements received
     */
    public boolean addSubscriptionKey(String key) {
        synchronized (this.subscriptionKeys) {
            this.subscriptionKeys.add(key);
        }
        return this.subscriptionKeys.size() >= this.subscriptionCount;
    }

    @Override
    public void run() {
        try {
            if (this.done.compareAndSet(false, true)) {
                this.storeTaskCompletionListener.taskCompleted();

                this.topicStore.addMessage(context, message);
                // apply any acks we have
                synchronized (this.subscriptionKeys) {
                    for (String key : this.subscriptionKeys) {
                        this.topicStore.doAcknowledge(context, key, this.message.getMessageId(), null);

                    }
                }
                kahaDBStore.removeTopicTask(this.topicStore, this.message.getMessageId());
                this.future.complete();
            } else {
                this.storeTaskCompletionListener.taskCanceled();
            }
        } catch (Throwable t) {
            this.future.setException(t);
            kahaDBStore.removeTopicTask(this.topicStore, this.message.getMessageId());
        }
    }

}
