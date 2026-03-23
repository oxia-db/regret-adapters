package io.regret.sdk.payload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public record IndexedGetPayload(
        @JsonProperty("index_name") String indexName,
        @JsonProperty("index_key") String indexKey
) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static IndexedGetPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, IndexedGetPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize IndexedGetPayload", e);
        }
    }
}
