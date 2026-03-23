package io.regret.sdk.payload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public record IndexedListPayload(
        @JsonProperty("index_name") String indexName,
        String start,
        String end
) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static IndexedListPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, IndexedListPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize IndexedListPayload", e);
        }
    }
}
