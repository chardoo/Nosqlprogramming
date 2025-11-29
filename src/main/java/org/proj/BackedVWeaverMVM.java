package org.proj;

import java.util.Map;
import java.util.TreeMap;

abstract class BackedVWeaverMVM<K extends Comparable<? super K>, P> implements MultiVersionMap<K, P> {
    private static class VWeaverNode {
        public long version;
        public String payload;
        public String next;
        public String vRidgy;
        public String kRidgy;  // Cross-list pointer
        public int level;

        public VWeaverNode() {}

        public VWeaverNode(long version, String payload, String next,
                           String vRidgy, String kRidgy, int level) {
            this.version = version;
            this.payload = payload;
            this.next = next;
            this.vRidgy = vRidgy;
            this.kRidgy = kRidgy;
            this.level = level;
        }
    }

    private final TreeMap<K, String> keyHeads; // Map from key to head node key
    private final KVStore store;
    private final Serializer<P> serializer;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;
    private long versionCounter;

    public BackedVWeaverMVM(KVStore store, Serializer<P> serializer) {
        this.keyHeads = new TreeMap<>();
        this.store = store;
        this.serializer = serializer;
        this.mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        this.versionCounter = 1;
    }

    @Override
    public long append(K key, P payload) {
        long version = versionCounter++;
        String newNodeKey = "vw_" + key + "_" + version;

        String headKey = keyHeads.get(key);

        // Calculate level
        int level = 0;
        if (headKey != null) {
            String currKey = headKey;
            while (currKey != null) {
                VWeaverNode curr = loadNode(currKey);
                if (curr.version <= version) break;
                if (curr.level == level) {
                    level++;
                }
                currKey = curr.next;
            }
        }

        // Find vRidgy
        String vRidgyKey = null;
        if (headKey != null) {
            String currKey = headKey;
            while (currKey != null) {
                VWeaverNode curr = loadNode(currKey);
                if (curr.level >= level) {
                    vRidgyKey = currKey;
                    break;
                }
                currKey = curr.next;
            }
        }

        // Find kRidgy - point to next key's visible version at current time
        String kRidgyKey = null;
        K nextKey = keyHeads.higherKey(key);
        if (nextKey != null) {
            String nextHeadKey = keyHeads.get(nextKey);
            if (nextHeadKey != null) {
                kRidgyKey = findVisibleNodeKey(nextHeadKey, version);
            }
        }

        VWeaverNode newNode = new VWeaverNode(version, serializer.serialize(payload),
                headKey, vRidgyKey, kRidgyKey, level);
        storeNode(newNodeKey, newNode);
        keyHeads.put(key, newNodeKey);
        return version;
    }

    @Override
    public P findVisible(K key, long timestamp) {
        String headKey = keyHeads.get(key);
        if (headKey == null) return null;

        String nodeKey = findVisibleNodeKey(headKey, timestamp);
        if (nodeKey == null) return null;

        VWeaverNode node = loadNode(nodeKey);
        return serializer.deserialize(node.payload);
    }

    @Override
    public Map<K, P> rangeSnapshot(K low, K high, long timestamp) {
        Map<K, P> result = new TreeMap<>();

        // Find first key in range
        K currentKey = keyHeads.ceilingKey(low);
        String currentNodeKey = null;

        if (currentKey != null && currentKey.compareTo(high) <= 0) {
            currentNodeKey = findVisibleNodeKey(keyHeads.get(currentKey), timestamp);
        }

        while (currentKey != null && currentKey.compareTo(high) <= 0 && currentNodeKey != null) {
            VWeaverNode node = loadNode(currentNodeKey);
            result.put(currentKey, serializer.deserialize(node.payload));

            // Use kRidgy to jump to next key
            if (node.kRidgy != null) {
                VWeaverNode kRidgyNode = loadNode(node.kRidgy);
                // Find which key this node belongs to
                currentKey = findKeyForNode(node.kRidgy);
                currentNodeKey = node.kRidgy;

                // Traverse to visible version if needed
                if (kRidgyNode.version > timestamp) {
                    currentNodeKey = findVisibleNodeKey(node.kRidgy, timestamp);
                }
            } else {
                // No kRidgy, move to next key manually
                currentKey = keyHeads.higherKey(currentKey);
                if (currentKey != null && currentKey.compareTo(high) <= 0) {
                    currentNodeKey = findVisibleNodeKey(keyHeads.get(currentKey), timestamp);
                } else {
                    break;
                }
            }
        }

        return result;
    }

    private String findVisibleNodeKey(String headKey, long timestamp) {
        String currKey = headKey;

        while (currKey != null) {
            VWeaverNode curr = loadNode(currKey);

            if (curr.version <= timestamp) {
                return currKey;
            }

            if (curr.vRidgy != null) {
                VWeaverNode vRidgyNode = loadNode(curr.vRidgy);
                if (vRidgyNode.version > timestamp) {
                    currKey = curr.vRidgy;
                    continue;
                }
            }

            currKey = curr.next;
        }

        return null;
    }

    private K findKeyForNode(String nodeKey) {
        // Extract key from node key format "vw_KEY_VERSION"
        for (Map.Entry<K, String> entry : keyHeads.entrySet()) {
            if (nodeKey.startsWith("vw_" + entry.getKey() + "_")) {
                return entry.getKey();
            }
        }
        return keyHeads.higherKey((K) keyHeads.firstKey()); // Fallback
    }

    private void storeNode(String key, VWeaverNode node) {
        try {
            store.put(key, mapper.writeValueAsString(node));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize node", e);
        }
    }

    private VWeaverNode loadNode(String key) {
        try {
            String json = store.get(key);
            return mapper.readValue(json, VWeaverNode.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize node", e);
        }
    }
}