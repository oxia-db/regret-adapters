package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record RangeScanPayload(String start, String end) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static RangeScanPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, RangeScanPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize RangeScanPayload", e);
        }
    }
}
