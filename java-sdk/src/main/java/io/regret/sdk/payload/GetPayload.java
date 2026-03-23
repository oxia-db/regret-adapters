package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record GetPayload(String key) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static GetPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, GetPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize GetPayload", e);
        }
    }
}
