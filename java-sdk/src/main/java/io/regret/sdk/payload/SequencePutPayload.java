package io.regret.sdk.payload;

import com.fasterxml.jackson.databind.ObjectMapper;

public record SequencePutPayload(String prefix, String value, long delta) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static SequencePutPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, SequencePutPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize SequencePutPayload", e);
        }
    }
}
