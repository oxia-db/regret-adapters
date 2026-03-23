package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record DeletePayload(String key) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static DeletePayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, DeletePayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize DeletePayload", e);
        }
    }
}
