package org.proj;

class FrugalSkiplist<P> implements VersionList<P> {
    private class Node {
        long version;
        P payload;
        Node next;      // Pointer to next smaller version
        Node vRidgy;    // Vertical ridge pointer
        int level;

        Node(long version, P payload) {
            this.version = version;
            this.payload = payload;
            this.next = null;
            this.vRidgy = null;
            this.level = 0;
        }
    }

    private Node head;


    @Override
    public void append(P p, long timestamp) {
        Node newNode = new Node(timestamp, p);

        if (head == null) {
            newNode.level = 0;
            head = newNode;
            return;
        }

        // Algorithm 1 from paper
        Node curr = head;
        int level = 0;

        while (curr != null && curr.version > timestamp) {
            if (curr.level == level) {
                level++;
            }
            curr = curr.next;
        }

        newNode.level = level;
        newNode.next = head;

        // Set vRidgy pointer
        curr = head;
        while (curr != null && curr.level < level) {
            curr = curr.next;
        }
        newNode.vRidgy = curr;

        head = newNode;
    }

    @Override
    public P findVisible(long timestamp) {
        // Algorithm 2 from paper
        if (head == null) return null;

        Node curr = head;

        while (curr != null) {
            if (curr.version <= timestamp) {
                return curr.payload;
            }

            // Use vRidgy to skip if possible
            if (curr.vRidgy != null && curr.vRidgy.version > timestamp) {
                curr = curr.vRidgy;
            } else {
                curr = curr.next;
            }
        }

        return null;
    }
}