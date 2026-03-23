package io.regret.sdk.payload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public record IndexedPutPayload(
        String key,
        String value,
        @JsonProperty("index_name") String indexName,
        @JsonProperty("index_key") String indexKey
) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static IndexedPutPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, IndexedPutPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize IndexedPutPayload", e);
        }
    }
}
