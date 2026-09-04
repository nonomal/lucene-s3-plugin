package com.github.wxk6b1203.store.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.IOException;

/**
 * Process-local {@link LockFactory} (wraps {@link SingleInstanceLockFactory}). The misleading
 * former name suggested S3-backed cross-node locking; that is neither implemented nor needed:
 * each shard has exactly one write owner in the cluster (enforced by ownerTerm/allocationEpoch
 * write fences in etcd), so the only lock the local IndexWriter requires is the in-process one
 * guarding against duplicate writers in this JVM.
 */
public class ProcessLocalLockFactory extends LockFactory {
    private final SingleInstanceLockFactory delegate = new SingleInstanceLockFactory();

    public ProcessLocalLockFactory() {
    }

    @Override
    public Lock obtainLock(Directory dir, String lockName) throws IOException {
        return delegate.obtainLock(dir, lockName);
    }
}
