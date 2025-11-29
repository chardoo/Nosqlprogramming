package org.proj;

import org.proj.KVStore;

public interface FlushableKVStore extends KVStore {
    void flushDB();
}
