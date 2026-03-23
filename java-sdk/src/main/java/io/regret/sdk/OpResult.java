package io.regret.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public record OpResult(String opId, String opType, String status, byte[] payload, String message) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static OpResult ok(String opId, String opType) {
        return new OpResult(opId, opType, "ok", null, null);
    }

    public static OpResult okWithVersion(String opId, String opType, long versionId) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("version_id", versionId);
            return new OpResult(opId, opType, "ok", MAPPER.writeValueAsBytes(node), null);
        } catch (Exception e) {
            return ok(opId, opType);
        }
    }

    public static OpResult notFound(String opId, String opType) {
        return new OpResult(opId, opType, "not_found", null, null);
    }

    public static OpResult versionMismatch(String opId, String opType) {
        return new OpResult(opId, opType, "version_mismatch", null, null);
    }

    public static OpResult get(String opId, String value, long versionId) {
        return get(opId, null, value, versionId);
    }

    public static OpResult get(String opId, String key, String value, long versionId) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            if (key != null) {
                node.put("key", key);
            }
            node.put("value", value);
            node.put("version_id", versionId);
            return new OpResult(opId, "get", "ok", MAPPER.writeValueAsBytes(node), null);
        } catch (Exception e) {
            return error(opId, "get", e.getMessage());
        }
    }

    public static OpResult rangeScan(String opId, List<RangeScanRecord> records) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            ArrayNode arr = node.putArray("records");
            for (RangeScanRecord r : records) {
                ObjectNode rec = arr.addObject();
                rec.put("key", r.key());
                rec.put("value", r.value());
                rec.put("version_id", r.versionId());
            }
            return new OpResult(opId, "range_scan", "ok", MAPPER.writeValueAsBytes(node), null);
        } catch (Exception e) {
            return error(opId, "range_scan", e.getMessage());
        }
    }

    public static OpResult list(String opId, List<String> keys) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            ArrayNode arr = node.putArray("keys");
            for (String k : keys) {
                arr.add(k);
            }
            return new OpResult(opId, "list", "ok", MAPPER.writeValueAsBytes(node), null);
        } catch (Exception e) {
            return error(opId, "list", e.getMessage());
        }
    }

    public static OpResult okWithKeyAndVersion(String opId, String opType, String key, long versionId) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("key", key);
            node.put("version_id", versionId);
            return new OpResult(opId, opType, "ok", MAPPER.writeValueAsBytes(node), null);
        } catch (Exception e) {
            return ok(opId, opType);
        }
    }

    public static OpResult error(String opId, String opType, String message) {
        return new OpResult(opId, opType, "error", null, message);
    }

    public record RangeScanRecord(String key, String value, long versionId) {}
}
