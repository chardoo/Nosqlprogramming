package org.proj;

public interface Serializer<T> {

    String serialize(T t);
    T deSerialize(String serializedT);
}
