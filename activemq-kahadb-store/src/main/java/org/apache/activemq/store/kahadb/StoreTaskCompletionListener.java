package org.apache.activemq.store.kahadb;

/**
 * Listener for completion of store tasks.  Note that taskCompleted() and taskCanceled() are mutually-exclusive; only
 * one or the other will be called for each task.
 *
 * Created by art on 11/17/18.
 */
public interface StoreTaskCompletionListener {
    /**
     * Called when the task completes without being canceled.
     */
    void taskCompleted();

    /**
     * Called when the task is canceled.
     */
    void taskCanceled();
}
