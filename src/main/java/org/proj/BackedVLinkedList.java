package org.proj;

class BackedVLinkedList<P> implements VersionList<P> {
    private static class SerializedNode {
        public long version;
        public String payload;
        public String next; // Key of next node

        public SerializedNode() {} // Default constructor for Jackson

        public SerializedNode(long version, String payload, String next) {
            this.version = version;
            this.payload = payload;
            this.next = next;
        }
    }

    private final KVStore store;
    private final Serializer<P> serializer;
    private String headKey;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;

    public BackedVLinkedList(KVStore store, Serializer<P> serializer) {
        this.store = store;
        this.serializer = serializer;
        this.headKey = null;
        this.mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    }

    @Override
    public void append( P payload, long version) {
        String key = "node_" + version;
        String serializedPayload = serializer.serialize(payload);
        SerializedNode node = new SerializedNode(version, serializedPayload, headKey);

        try {
            String nodeJson = mapper.writeValueAsString(node);
            store.put(key, nodeJson);
            headKey = key;
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize node", e);
        }
    }

    @Override
    public P findVisible(long timestamp) {
        String currentKey = headKey;

        while (currentKey != null) {
            try {
                String nodeJson = store.get(currentKey);
                if (nodeJson == null) break;

                SerializedNode node = mapper.readValue(nodeJson, SerializedNode.class);
                if (node.version <= timestamp) {
                    return serializer.deSerialize(node.payload);
                }
                currentKey = node.next;
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize node", e);
            }
        }
        return null;
    }
}