package io.regret.sdk;

import java.util.List;

public record Batch(String batchId, String traceId, List<Item> items) {}
