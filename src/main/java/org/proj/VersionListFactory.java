package org.proj;

public interface VersionListFactory<P> {
    VersionList<P> create(KVStore store, Serializer<P> serializer);

}
