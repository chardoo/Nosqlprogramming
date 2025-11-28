package org.proj;

class VLinkedList<P> implements VersionList<P> {
    private class Node {
        long version;
        P payload;
        Node next;

        Node(long version, P payload, Node next) {
            this.version = version;
            this.payload = payload;
            this.next = next;
        }
    }

    private Node head;

    @Override
    public void append(P payload, long version) {
        head = new Node(version, payload, head);
    }


    @Override
    public P findVisible(long timestamp) {
        Node current = head;
        while (current != null) {
            if (current.version <= timestamp) {
                return current.payload;
            }
            current = current.next;
        }
        return null;
    }
}
