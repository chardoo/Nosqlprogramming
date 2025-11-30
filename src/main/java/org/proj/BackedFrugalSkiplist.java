package org.proj;

public class BackedFrugalSkiplist<P> implements VersionList<P>   {
  private static class SerializedNode {
        public long version;
        public String payload;
        public String next;
        public String vRidgy;
        public int level;

        public SerializedNode() {}

        public SerializedNode(long version, String payload, String next, String vRidgy, int level) {
            this.version = version;
            this.payload = payload;
            this.next = next;
            this.vRidgy = vRidgy;
            this.level = level;
        }
    }

    private final KVStore store;
    private final Serializer<P> serializer;
    private String headKey;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;

    public BackedFrugalSkiplist(KVStore store, Serializer<P> serializer) {
        this.store = store;
        this.serializer = serializer;
        this.headKey = null;
        this.mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    }
    @Override
    public void append(P p, long timestamp) {
        String newKey = "fsnode_" + timestamp;
        if (headKey == null) {
            SerializedNode node = new SerializedNode(timestamp, serializer.serialize(p),
                    null, null, 0);
            storeNode(newKey, node);
            headKey = newKey;
            return;
        }

        // Calculate level
        String currKey = headKey;
        int level = 0;

        while (currKey != null) {
            SerializedNode curr = loadNode(currKey);
            if (curr.version <= timestamp) break;
            if (curr.level == level) {
                level++;
            }
            currKey = curr.next;
        }

        // Find vRidgy
        currKey = headKey;
        String vRidgyKey = null;
        while (currKey != null) {
            SerializedNode curr = loadNode(currKey);
            if (curr.level >= level) {
                vRidgyKey = currKey;
                break;
            }
            currKey = curr.next;
        }

        SerializedNode newNode = new SerializedNode(timestamp, serializer.serialize(p),
                headKey, vRidgyKey, level);
        storeNode(newKey, newNode);
        headKey = newKey;

    }

    @Override
    public P findVisible(long timestamp) {
        if (headKey == null) return null;

        String currKey = headKey;

        while (currKey != null) {
            SerializedNode curr = loadNode(currKey);

            if (curr.version <= timestamp) {
                return serializer.deserialize(curr.payload);
            }

            if (curr.vRidgy != null) {
                SerializedNode vRidgyNode = loadNode(curr.vRidgy);
                if (vRidgyNode.version > timestamp) {
                    currKey = curr.vRidgy;
                    continue;
                }
            }

            currKey = curr.next;
        }

        return null;
    }

    private void storeNode(String key, SerializedNode node) {
        try {
            store.put(key, mapper.writeValueAsString(node));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize node", e);
        }
    }

    private SerializedNode loadNode(String key) {
        try {
            String json = store.get(key);
            return mapper.readValue(json, SerializedNode.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize node", e);
        }
    }
}