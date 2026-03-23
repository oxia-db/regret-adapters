package io.regret.sdk.payload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public record CasPayload(
        String key,
        @JsonProperty("expected_version_id") long expectedVersionId,
        @JsonProperty("new_value") String newValue
) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static CasPayload fromBytes(byte[] payload) {
        try {
            return MAPPER.readValue(payload, CasPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize CasPayload", e);
        }
    }
}
