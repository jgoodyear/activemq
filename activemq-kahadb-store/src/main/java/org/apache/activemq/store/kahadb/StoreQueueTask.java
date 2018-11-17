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
import org.apache.activemq.store.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class StoreQueueTask implements Runnable, StoreTask {

    static final Logger LOG = LoggerFactory.getLogger(StoreQueueTask.class);
    protected final Message message;
    protected final ConnectionContext context;
    protected final KahaDBStore kahaDBStore;
    protected final KahaDBMessageStore store;
    protected final StoreQueueTask.InnerFutureTask future;
    protected final AtomicBoolean done = new AtomicBoolean();
    protected final AtomicBoolean locked = new AtomicBoolean();

    public StoreQueueTask(KahaDBStore kahaDBStore, KahaDBMessageStore store, ConnectionContext context, Message message) {
        this.kahaDBStore = kahaDBStore;
        this.store = store;
        this.context = context;
        this.message = message;
        this.future = new StoreQueueTask.InnerFutureTask(this);
    }

    public ListenableFuture<Object> getFuture() {
        return this.future;
    }

    @Override
    public boolean cancel() {
        if (this.done.compareAndSet(false, true)) {
            return this.future.cancel(false);
        }
        return false;
    }

    @Override
    public void aquireLocks() {
        if (this.locked.compareAndSet(false, true)) {
            try {
                kahaDBStore.globalQueueSemaphore.acquire();
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
            store.releaseLocalAsyncLock();
            kahaDBStore.globalQueueSemaphore.release();
            message.decrementReferenceCount();
        }
    }

    @Override
    public void run() {
        this.store.doneTasks++;
        try {
            if (this.done.compareAndSet(false, true)) {
                this.store.addMessage(context, message);
                kahaDBStore.removeQueueTask(this.store, this.message.getMessageId());
                this.future.complete();
            } else if (kahaDBStore.cancelledTaskModMetric > 0 && (++this.store.canceledTasks) % kahaDBStore.cancelledTaskModMetric == 0) {
                System.err.println(this.store.dest.getName() + " cancelled: "
                        + (this.store.canceledTasks / this.store.doneTasks) * 100);
                this.store.canceledTasks = this.store.doneTasks = 0;
            }
        } catch (Throwable t) {
            this.future.setException(t);
            kahaDBStore.removeQueueTask(this.store, this.message.getMessageId());
        }
    }

    protected Message getMessage() {
        return this.message;
    }

    protected class InnerFutureTask extends FutureTask<Object> implements ListenableFuture<Object>  {

        private final AtomicReference<Runnable> listenerRef = new AtomicReference<>();

        public InnerFutureTask(Runnable runnable) {
            super(runnable, null);
        }

        public void setException(final Throwable e) {
            super.setException(e);
        }

        public void complete() {
            super.set(null);
        }

        @Override
        public void done() {
            fireListener();
        }

        @Override
        public void addListener(Runnable listener) {
            this.listenerRef.set(listener);
            if (isDone()) {
                fireListener();
            }
        }

        private void fireListener() {
            Runnable listener = listenerRef.getAndSet(null);
            if (listener != null) {
                try {
                    listener.run();
                } catch (Exception ignored) {
                    LOG.warn("Unexpected exception from future {} listener callback {}", this, listener, ignored);
                }
            }
        }
    }
}