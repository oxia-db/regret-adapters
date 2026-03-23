package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record EphemeralPutPayload(String key, String value) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static EphemeralPutPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, EphemeralPutPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize EphemeralPutPayload", e);
        }
    }
}
