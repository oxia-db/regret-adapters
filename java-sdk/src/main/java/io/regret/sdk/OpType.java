package io.regret.sdk;

public enum OpType {
    PUT("put"),
    GET("get"),
    DELETE("delete"),
    DELETE_RANGE("delete_range"),
    LIST("list"),
    RANGE_SCAN("range_scan"),
    CAS("cas"),
    EPHEMERAL_PUT("ephemeral_put"),
    INDEXED_PUT("indexed_put"),
    INDEXED_GET("indexed_get"),
    INDEXED_LIST("indexed_list"),
    INDEXED_RANGE_SCAN("indexed_range_scan"),
    SEQUENCE_PUT("sequence_put"),
    FENCE("fence");

    private final String value;

    OpType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static OpType fromString(String s) {
        for (OpType t : values()) {
            if (t.value.equals(s)) return t;
        }
        throw new IllegalArgumentException("Unknown op type: " + s);
    }
}
