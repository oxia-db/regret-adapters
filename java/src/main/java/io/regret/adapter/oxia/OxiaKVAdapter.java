package io.regret.adapter.oxia;

import io.regret.sdk.*;
import io.regret.sdk.OpType;
import io.regret.sdk.payload.*;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.ListOption;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.RangeScanOption;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class OxiaKVAdapter implements Adapter {

    private static final Logger LOG = LoggerFactory.getLogger(OxiaKVAdapter.class);

    private final SyncOxiaClient client;

    public OxiaKVAdapter() {
        String oxiaAddr = System.getenv("OXIA_ADDR");
        String namespace = System.getenv("OXIA_NAMESPACE");

        if (oxiaAddr == null) {
            throw new IllegalStateException("OXIA_ADDR env var is required");
        }

        LOG.info("Connecting to Oxia at {} namespace={}", oxiaAddr, namespace);

        var builder = OxiaClientBuilder.create(oxiaAddr);
        if (namespace != null && !namespace.isEmpty()) {
            builder.namespace(namespace);
        }
        try {
            this.client = builder.syncClient();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Oxia client", e);
        }
    }

    @Override
    public OpResult executeOp(Operation op) {
        LOG.debug("  op={} id={} payload={}", op.opType(), op.opId(),
                op.payload() != null ? new String(op.payload(), StandardCharsets.UTF_8) : "null");
        try {
            return switch (op.opType()) {
                case PUT -> {
                    var p = PutPayload.fromBytes(op.payload());
                    client.put(p.key(), p.value().getBytes(StandardCharsets.UTF_8));
                    yield OpResult.ok(op.opId(), OpType.PUT.value());
                }
                case DELETE -> {
                    var p = DeletePayload.fromBytes(op.payload());
                    boolean existed = client.delete(p.key());
                    yield existed ? OpResult.ok(op.opId(), OpType.DELETE.value())
                            : OpResult.notFound(op.opId(), OpType.DELETE.value());
                }
                case DELETE_RANGE -> {
                    var p = DeleteRangePayload.fromBytes(op.payload());
                    client.deleteRange(p.start(), p.end());
                    yield OpResult.ok(op.opId(), OpType.DELETE_RANGE.value());
                }
                case CAS -> {
                    var p = CasPayload.fromBytes(op.payload());
                    try {
                        client.put(p.key(), p.newValue().getBytes(StandardCharsets.UTF_8),
                                Set.of(PutOption.IfVersionIdEquals(p.expectedVersionId())));
                        yield OpResult.ok(op.opId(), OpType.CAS.value());
                    } catch (UnexpectedVersionIdException e) {
                        yield OpResult.versionMismatch(op.opId(), OpType.CAS.value());
                    }
                }
                case GET -> {
                    var p = GetPayload.fromBytes(op.payload());
                    GetResult res = client.get(p.key());
                    if (res == null) {
                        yield OpResult.notFound(op.opId(), OpType.GET.value());
                    } else {
                        yield OpResult.get(op.opId(),
                                new String(res.getValue(), StandardCharsets.UTF_8),
                                res.getVersion().versionId());
                    }
                }
                case RANGE_SCAN -> {
                    var p = RangeScanPayload.fromBytes(op.payload());
                    var keys = client.list(p.start(), p.end());
                    var records = new ArrayList<OpResult.RangeScanRecord>();
                    for (String key : keys) {
                        GetResult r = client.get(key);
                        if (r != null) {
                            records.add(new OpResult.RangeScanRecord(key,
                                    new String(r.getValue(), StandardCharsets.UTF_8),
                                    r.getVersion().versionId()));
                        }
                    }
                    yield OpResult.rangeScan(op.opId(), records);
                }
                case LIST -> {
                    var p = ListPayload.fromBytes(op.payload());
                    var keys = client.list(p.start(), p.end());
                    yield OpResult.list(op.opId(), keys);
                }
                case EPHEMERAL_PUT -> {
                    var p = EphemeralPutPayload.fromBytes(op.payload());
                    client.put(p.key(), p.value().getBytes(StandardCharsets.UTF_8),
                            Set.of(PutOption.AsEphemeralRecord));
                    yield OpResult.ok(op.opId(), OpType.EPHEMERAL_PUT.value());
                }
                case INDEXED_PUT -> {
                    var p = IndexedPutPayload.fromBytes(op.payload());
                    client.put(p.key(), p.value().getBytes(StandardCharsets.UTF_8),
                            Set.of(PutOption.SecondaryIndex(p.indexName(), p.indexKey())));
                    yield OpResult.ok(op.opId(), OpType.INDEXED_PUT.value());
                }
                case INDEXED_GET -> {
                    var p = IndexedGetPayload.fromBytes(op.payload());
                    // GetOption does not support UseIndex; use list with UseIndex to
                    // resolve the primary key, then get the record by primary key.
                    var keys = client.list(p.indexKey(), p.indexKey() + "\0",
                            Set.of(ListOption.UseIndex(p.indexName())));
                    if (keys.isEmpty()) {
                        yield OpResult.notFound(op.opId(), OpType.INDEXED_GET.value());
                    } else {
                        GetResult res = client.get(keys.get(0));
                        if (res == null) {
                            yield OpResult.notFound(op.opId(), OpType.INDEXED_GET.value());
                        } else {
                            yield OpResult.get(op.opId(),
                                    new String(res.getValue(), StandardCharsets.UTF_8),
                                    res.getVersion().versionId());
                        }
                    }
                }
                case INDEXED_LIST -> {
                    var p = IndexedListPayload.fromBytes(op.payload());
                    var keys = client.list(p.start(), p.end(),
                            Set.of(ListOption.UseIndex(p.indexName())));
                    yield OpResult.list(op.opId(), keys);
                }
                case INDEXED_RANGE_SCAN -> {
                    var p = IndexedRangeScanPayload.fromBytes(op.payload());
                    var iterable = client.rangeScan(p.start(), p.end(),
                            Set.of(RangeScanOption.UseIndex(p.indexName())));
                    var records = new ArrayList<OpResult.RangeScanRecord>();
                    for (GetResult r : iterable) {
                        records.add(new OpResult.RangeScanRecord(
                                r.getKey(),
                                new String(r.getValue(), StandardCharsets.UTF_8),
                                r.getVersion().versionId()));
                    }
                    yield OpResult.rangeScan(op.opId(), records);
                }
                case SEQUENCE_PUT -> {
                    var p = SequencePutPayload.fromBytes(op.payload());
                    client.put(p.prefix(), p.value().getBytes(StandardCharsets.UTF_8),
                            Set.of(PutOption.SequenceKeysDeltas(List.of(p.delta())),
                                    PutOption.PartitionKey(p.prefix())));
                    yield OpResult.ok(op.opId(), OpType.SEQUENCE_PUT.value());
                }
                default -> OpResult.error(op.opId(), op.opType().value(),
                        "unknown op type: " + op.opType());
            };
        } catch (Exception e) {
            return OpResult.error(op.opId(), op.opType().value(), e.getMessage());
        }
    }

    @Override
    public List<io.regret.sdk.Record> readState(String keyPrefix) throws Exception {
        LOG.info("readState prefix={}", keyPrefix);
        var records = new ArrayList<io.regret.sdk.Record>();
        for (GetResult res : client.rangeScan(keyPrefix, keyPrefix + "\uffff")) {
            records.add(io.regret.sdk.Record.builder()
                    .key(res.getKey())
                    .value(res.getValue())
                    .metadata(Map.of(
                            "version_id",
                            String.valueOf(res.getVersion().versionId())))
                    .build());
        }
        LOG.info("readState returned {} records", records.size());
        return records;
    }

    @Override
    public void cleanup(String keyPrefix) throws Exception {
        client.deleteRange(keyPrefix, keyPrefix + "~");
    }

    public static void main(String[] args) throws Exception {
        RegretAdapterServer.serve(new OxiaKVAdapter());
    }
}
