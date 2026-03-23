package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record PutPayload(String key, String value) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static PutPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, PutPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize PutPayload", e);
        }
    }
}
