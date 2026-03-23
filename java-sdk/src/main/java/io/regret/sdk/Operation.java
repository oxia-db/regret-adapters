package io.regret.sdk;

public record Operation(String opId, OpType opType, byte[] payload) {}
