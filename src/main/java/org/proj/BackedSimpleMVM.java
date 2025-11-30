package org.proj;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

class BackedSimpleMVM<K extends Comparable<? super K>, P> implements MultiVersionMap<K, P> {
        private final TreeMap<K, VersionList<P>> tree;
        private final VersionListFactory<P> factory;
        private long versionCounter;

        public BackedSimpleMVM(VersionListFactory<P> factory) {
            this.tree = new TreeMap<>();
            this.factory = factory;
            this.versionCounter = 1;
        }

    @Override
    public Map.Entry<K, P> get(K k, long t) {
        return null;
    }

    @Override
    public long append(K k, P p) {
        return 0;
    }

    @Override
    public Iterator<Map.Entry<K, P>> rangeSnapshot(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long timestamp) {
        return null;
    }

    @Override
    public Iterator<Map.Entry<K, P>> snapshot(long timestamp) {
        return null;
    }

    @Override
    public P findVisible(K key, long timestamp) {
        VersionList<P> vlist = tree.get(key);
        return vlist != null ? vlist.findVisible(timestamp) : null;
    }

    @Override
    public Map<K, P> rangeSnapshot(K low, K high, long timestamp) {
        Map<K, P> result = new TreeMap<>();
        for (Map.Entry<K, VersionList<P>> entry : tree.subMap(low, true, high, true).entrySet()) {
            P visible = entry.getValue().findVisible(timestamp);
            if (visible != null) {
                result.put(entry.getKey(), visible);
            }
        }
        return result;
    }


}

