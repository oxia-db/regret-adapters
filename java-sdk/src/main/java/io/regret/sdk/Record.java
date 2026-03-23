package io.regret.sdk;

import java.util.HashMap;
import java.util.Map;

public class Record {
    private final String key;
    private final byte[] value;
    private final Map<String, String> metadata;

    private Record(Builder builder) {
        this.key = builder.key;
        this.value = builder.value;
        this.metadata = builder.metadata;
    }

    public String getKey() { return key; }
    public byte[] getValue() { return value; }
    public Map<String, String> getMetadata() { return metadata; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String key;
        private byte[] value;
        private Map<String, String> metadata = new HashMap<>();

        public Builder key(String key) { this.key = key; return this; }
        public Builder value(byte[] value) { this.value = value; return this; }
        public Builder metadata(Map<String, String> metadata) { this.metadata = metadata; return this; }
        public Record build() { return new Record(this); }
    }
}
