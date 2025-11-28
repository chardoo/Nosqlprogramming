package org.proj;

public interface KVStore {
    void put(String storeKey, String storeValue);
    String get(String storeKey);
}
