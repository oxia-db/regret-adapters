package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record DeleteRangePayload(String start, String end) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static DeleteRangePayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, DeleteRangePayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize DeleteRangePayload", e);
        }
    }
}
