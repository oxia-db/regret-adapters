package io.regret.sdk;

import java.util.List;

public record BatchResponse(String batchId, List<OpResult> results) {}
