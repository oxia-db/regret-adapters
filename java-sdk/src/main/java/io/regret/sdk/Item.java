package io.regret.sdk;

public sealed interface Item {
    record Op(Operation op) implements Item {}
    record Fence() implements Item {}
}
