package io.github.oxia.worker.engine.oxia;

import io.github.oxia.okk.worker.engine.Engine;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionEphemeral;
import io.oxia.okk.proto.v1.Assertion;
import io.oxia.okk.proto.v1.ExecuteCommand;
import io.oxia.okk.proto.v1.ExecuteResponse;
import io.oxia.okk.proto.v1.KeyComparisonType;
import io.oxia.okk.proto.v1.NotificationType;
import io.oxia.okk.proto.v1.Operation;
import io.oxia.okk.proto.v1.OperationDelete;
import io.oxia.okk.proto.v1.OperationDeleteRange;
import io.oxia.okk.proto.v1.OperationGet;
import io.oxia.okk.proto.v1.OperationList;
import io.oxia.okk.proto.v1.OperationPut;
import io.oxia.okk.proto.v1.OperationScan;
import io.oxia.okk.proto.v1.Precondition;
import io.oxia.okk.proto.v1.Record;
import io.oxia.okk.proto.v1.Status;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OxiaEngine implements Engine {

    private Options options;
    private AsyncOxiaClient oxiaClient;


    private boolean watchedNotification;
    private BlockingDeque<Notification> notifications;

    @Override
    public void init() {
        options = Options.fromEnv();
        log.info("Loading oxia engine. options: {}", options);
        initClient();
        watchedNotification = false;
        notifications = new LinkedBlockingDeque<>();
    }


    private void initClient() {
        oxiaClient = OxiaClientBuilder.create(options.serviceURL())
                .namespace(options.namespace())
                .asyncClient().join();
    }

    private void maybeInitNotifications() {
        if (watchedNotification) {
            return;
        }
        if (oxiaClient == null) {
            initClient();
        }
        oxiaClient.notifications(notification -> {
            log.info("receive the notification {}", notification);
            notifications.add(notification);
        });
        watchedNotification = true;
    }


    @SneakyThrows
    private ExecuteResponse processPut(Operation operation) {
        if (operation.hasPrecondition()) {
            final Precondition precondition = operation.getPrecondition();
            if (precondition.getBypassIfAssertKeyExist()) {
                // Idempotent operations
                if (operation.hasAssertion()) {
                    final Assertion assertion = operation.getAssertion();
                    if (!assertion.getRecordsList().isEmpty()) {
                        final Record expectRecord = assertion.getRecords(0);
                        final GetResult result = oxiaClient.get(expectRecord.getKey()).join();
                        if (result != null) {
                            // the key might be exists
                            final String getKey = result.key();
                            final byte[] getValue = result.value();
                            if (getKey.equals(expectRecord.getKey())
                                    && Arrays.equals(getValue, expectRecord.getValue().toByteArray())) {
                                log.info("[Put][{}] The precondition BypassIfAssertKeyExist is met.", operation.getSequence());
                                return ExecuteResponse.newBuilder()
                                        .setStatus(Status.Ok)
                                        .build();
                            } else {
                                log.warn("[Put][{}] Assertion failure, mismatched key or value. expect: key={} vlaue={} actual: key={} value={}",
                                        operation.getSequence(), expectRecord.getKey(), expectRecord.getValue(), getKey, getValue);
                                return ExecuteResponse.newBuilder()
                                        .setStatus(Status.AssertionFailure)
                                        .setStatusInfo("mismatched key or value.")
                                        .build();
                            }
                        }
                    }
                }
            }
            if (precondition.getWatchNotification()) {
                maybeInitNotifications();
            }
        }

        final OperationPut put = operation.getPut();
        final var optionSet = new HashSet<PutOption>();
        if (put.hasPartitionKey()) {
            optionSet.add(PutOption.PartitionKey(put.getPartitionKey()));
        }
        if (put.getSequenceKeyDeltaCount() > 0) {
            optionSet.add(PutOption.SequenceKeysDeltas(put.getSequenceKeyDeltaList()));
        }
        if (put.getEphemeral()) {
            optionSet.add(OptionEphemeral.AsEphemeralRecord);
        }
        if (put.hasExpectedVersionId()) {
            long expectedVersion = put.getExpectedVersionId();
            if (expectedVersion == -1) {
                optionSet.add(PutOption.IfRecordDoesNotExist);
            } else {
                optionSet.add(PutOption.IfVersionIdEquals(expectedVersion));
            }
        }

        // Check if this operation expects a version conflict
        boolean expectConflict = operation.hasAssertion()
                && operation.getAssertion().hasExpectVersionConflict()
                && operation.getAssertion().getExpectVersionConflict();

        final PutResult result;
        try {
            result = oxiaClient.put(put.getKey(), put.getValue().toByteArray(), optionSet).join();
        } catch (Exception ex) {
            Throwable cause = ex;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            boolean isVersionConflict = cause instanceof KeyAlreadyExistsException
                    || cause instanceof UnexpectedVersionIdException;
            if (expectConflict && isVersionConflict) {
                log.info("[Put][{}] Expected version conflict occurred as expected: {}",
                        operation.getSequence(), cause.getMessage());
                return ExecuteResponse.newBuilder()
                        .setStatus(Status.Ok)
                        .build();
            }
            throw ex;
        }

        if (expectConflict) {
            log.warn("[Put][{}] Expected version conflict but put succeeded", operation.getSequence());
            return ExecuteResponse.newBuilder()
                    .setStatus(Status.AssertionFailure)
                    .setStatusInfo("expected version conflict but put succeeded")
                    .build();
        }

        // Return version_id from successful put
        var responseBuilder = ExecuteResponse.newBuilder().setStatus(Status.Ok);
        if (result.version() != null) {
            responseBuilder.setVersionId(result.version().versionId());
        }

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (!assertion.getRecordsList().isEmpty()) {
                final Record expectRecord = assertion.getRecords(0);

                final String putKey = result.key();
                final String expectKey = expectRecord.getKey();
                if (!putKey.equals(expectKey)) {
                    log.warn("[Put][{}] Assertion failure, mismatched key. expect: key={} actual: key={}",
                            operation.getSequence(), expectRecord.getKey(), putKey);
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatched key.")
                            .build();
                }
            }
            if (assertion.hasNotification()) {
                final var notification = assertion.getNotification();
                final NotificationType expectType = notification.getType();
                final String expectKey = notification.getKey();
                // Derive key prefix to filter out notifications from other test cases
                final String keyPrefix = expectKey != null ? expectKey.substring(0, expectKey.indexOf('/', 1) + 1) : null;

                final var actualNotification = pollMatchingNotification(keyPrefix, 3, TimeUnit.MINUTES);
                if (actualNotification instanceof Notification.KeyCreated an) {
                    if (expectType != NotificationType.KEY_CREATED || !expectKey.equals(an.key())) {
                        return ExecuteResponse.newBuilder()
                                .setStatus(Status.AssertionFailure)
                                .setStatusInfo("mismatched notification.")
                                .build();
                    }

                } else if (actualNotification instanceof Notification.KeyModified an) {
                    if (expectType != NotificationType.KEY_MODIFIED || !expectKey.equals(an.key())) {
                        return ExecuteResponse.newBuilder()
                                .setStatus(Status.AssertionFailure)
                                .setStatusInfo("mismatched notification.")
                                .build();
                    }
                } else {
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatched notification.")
                            .build();
                }
            }
        }

        return responseBuilder.build();
    }

    private ExecuteResponse processScan(Operation operation) {
        final OperationScan scanOp = operation.getScan();

//        final CompletableFuture<Void> future = new CompletableFuture<>();
//        final List<GetResult> results = new ArrayList<>();
//        oxiaClient.rangeScan(scanOp.getKeyStart(), scanOp.getKeyEnd(), new RangeScanConsumer() {
//            @Override
//            public void onNext(GetResult result) {
//                results.add(result);
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                future.completeExceptionally(throwable);
//            }
//
//            @Override
//            public void onCompleted() {
//                future.complete(null);
//            }
//        });
//        future.join();
//
//        if (operation.hasAssertion()) {
//            final Assertion assertion = operation.getAssertion();
//            if (!assertion.getRecordsList().isEmpty()) {
//                log.info("[Scan][{}] Check the assertion records", operation.getSequence());
//
//                log.info("[Scan][{}] Assertion successful", operation.getSequence());
//            }
//        }

        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processGet(String testcase, Operation operation) {
        final OperationGet get = operation.getGet();
        final String key = get.getKey();
        final KeyComparisonType comparisonType = get.getComparisonType();

        final Set<GetOption> getOptions = new HashSet<>();
        switch (comparisonType) {
            case EQUAL -> getOptions.add(GetOption.ComparisonEqual);
            case FLOOR -> getOptions.add(GetOption.ComparisonFloor);
            case LOWER -> getOptions.add(GetOption.ComparisonLower);
            case HIGHER -> getOptions.add(GetOption.ComparisonHigher);
            case CEILING -> getOptions.add(GetOption.ComparisonCeiling);
            default -> {
            }
        }
        GetResult getResult = oxiaClient.get(key, getOptions).join();

        // avoid expose internal keys
        if (getResult != null) {
            final String getKey = getResult.key();
            if (getKey.startsWith("__oxia/")  //
                    || !getKey.startsWith(testcase)) {
                getResult = null;
            }
        }

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (assertion.hasEmptyRecords() && assertion.getEmptyRecords()) {
                log.info("[Get][{}] Check the assertion empty records", operation.getSequence());
                if (getResult != null) {
                    log.warn("[Get][{}] Assertion failure, expect empty record. actual: key={} value={}",
                            operation.getSequence(), getResult.key(), getResult.value());
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatch key or value")
                            .build();
                }
                log.info("[Get][{}] Assertion successful", operation.getSequence());
            }
            final List<Record> recordsList = assertion.getRecordsList();
            if (!recordsList.isEmpty()) {
                final String actualKey = getResult.key();
                final byte[] actualValue = getResult.value();
                log.info("[Get][{}] Check the assertion records", operation.getSequence());
                final Record expectRecord = recordsList.get(0);
                if (!expectRecord.getKey().equals(actualKey) || !Arrays.equals(expectRecord.getValue().toByteArray(), actualValue)) {
                    log.warn("[Get][{}] Assertion failure, mismatched key or value. expect: key={} value={} actual: key={} value={}",
                            operation.getSequence(), expectRecord.getKey(), expectRecord.getValue(), actualKey, actualValue);
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatch key or value")
                            .build();
                }
                log.info("[Get][{}] Assertion successful", operation.getSequence());
            }
        }
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processList(Operation operation) {
        final OperationList listOp = operation.getList();
        final List<String> actualKeys = oxiaClient.list(listOp.getKeyStart(), listOp.getKeyEnd()).join();

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (assertion.hasEventuallyEmpty() || assertion.hasEmptyRecords()) {
                log.info("[List][{}] Check the empty assertion", operation.getSequence());
                if (!actualKeys.isEmpty()) {
                    log.warn("[List][{}] Assertion failure", operation.getSequence());
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("expect empty, but the actual is %s ".formatted(String.join(",", actualKeys)))
                            .build();
                }
                log.info("[List][{}] Assertion successful", operation.getSequence());
            }
            if (!assertion.getRecordsList().isEmpty()) {
                log.info("[List][{}] Check the assertion records", operation.getSequence());
                final List<String> expectKeys = assertion.getRecordsList().stream().map(Record::getKey).toList();
                if (!actualKeys.equals(expectKeys)) {
                    log.warn("[List][{}] Assertion failure", operation.getSequence());
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("different keys expect %s, but the actual is %s ".formatted(
                                    String.join(",", expectKeys),
                                    String.join(",", actualKeys)
                            ))
                            .build();
                }
                log.info("[List][{}] Assertion successful", operation.getSequence());
            }
        }
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    @SneakyThrows
    private ExecuteResponse processSessionRestart(Operation __) {
        oxiaClient.close();
        initClient();
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    @SneakyThrows
    private ExecuteResponse processDelete(Operation operation) {
        final OperationDelete delete = operation.getDelete();
        final String key = delete.getKey();
        if (operation.hasPrecondition()) {
            final Precondition precondition = operation.getPrecondition();
            if (precondition.getWatchNotification()) {
                maybeInitNotifications();
            }
        }

        oxiaClient.delete(key).join();

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (assertion.hasNotification()) {
                final var notification = assertion.getNotification();
                final NotificationType expectType = notification.getType();
                final String expectKey = notification.getKey();
                final String keyPrefix = expectKey != null ? expectKey.substring(0, expectKey.indexOf('/', 1) + 1) : null;

                final Notification actualNotification = pollMatchingNotification(keyPrefix, 3, TimeUnit.MINUTES);
                if (actualNotification instanceof Notification.KeyDeleted an) {
                    if (expectType != NotificationType.KEY_DELETED || !expectKey.equals(an.key())) {
                        return ExecuteResponse.newBuilder()
                                .setStatus(Status.AssertionFailure)
                                .setStatusInfo("mismatched notification.")
                                .build();
                    }
                } else {
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatched notification.")
                            .build();
                }
            }
        }
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    @SneakyThrows
    private ExecuteResponse processDeleteRange(Operation operation) {
        final OperationDeleteRange deleteRange = operation.getDeleteRange();
        final String keyStart = deleteRange.getKeyStart();
        final String keyEnd = deleteRange.getKeyEnd();

        if (operation.hasPrecondition()) {
            final Precondition precondition = operation.getPrecondition();
            if (precondition.getWatchNotification()) {
                maybeInitNotifications();
            }
        }

        oxiaClient.deleteRange(keyStart, keyEnd).join();

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (assertion.hasNotification()) {
                final var notification = assertion.getNotification();
                final NotificationType expectType = notification.getType();
                final String notifKeyStart = notification.getKeyStart();
                final String keyPrefix = notifKeyStart != null ? notifKeyStart.substring(0, notifKeyStart.indexOf('/', 1) + 1) : null;

                final Notification actualNotification = pollMatchingNotification(keyPrefix, 3, TimeUnit.MINUTES);
                if (actualNotification instanceof Notification.KeyRangeDelete an) {
                    if (expectType != NotificationType.KEY_RANGE_DELETED) {
                        return ExecuteResponse.newBuilder()
                                .setStatus(Status.AssertionFailure)
                                .setStatusInfo("mismatched notification.")
                                .build();
                    }
                    final String notificationKeyStart = notification.getKeyStart();
                    final String notificationKeyEnd = notification.getKeyEnd();

                    if (!notificationKeyStart.equals(an.startKeyInclusive()) ||
                            !notificationKeyEnd.equals(an.endKeyExclusive())) {
                        return ExecuteResponse.newBuilder()
                                .setStatus(Status.AssertionFailure)
                                .setStatusInfo("mismatched notification.")
                                .build();
                    }
                } else {
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatched notification.")
                            .build();
                }
            }
        }

        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }


    /**
     * Poll the notification queue, skipping notifications that don't match the given key prefix.
     * This prevents cross-test-case notification pollution when multiple tests share one worker.
     */
    @SneakyThrows
    private Notification pollMatchingNotification(String keyPrefix, long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            Notification n = notifications.poll(remaining, TimeUnit.NANOSECONDS);
            if (n == null) return null;
            String notifKey = getNotificationKey(n);
            if (keyPrefix == null || notifKey == null || notifKey.startsWith(keyPrefix)) {
                return n;
            }
            // Skip notification from a different test case
            log.debug("Skipping notification for different prefix: {} (expected prefix: {})", notifKey, keyPrefix);
        }
        return null;
    }

    private String getNotificationKey(Notification n) {
        if (n instanceof Notification.KeyCreated kc) return kc.key();
        if (n instanceof Notification.KeyModified km) return km.key();
        if (n instanceof Notification.KeyDeleted kd) return kd.key();
        if (n instanceof Notification.KeyRangeDelete kr) return kr.startKeyInclusive();
        return null;
    }

    @Override
    public ExecuteResponse onCommand(ExecuteCommand command) {
        final Operation operation = command.getOperation();
        try {
            return switch (operation.getOperationCase()) {
                case GET -> processGet(command.getTestcase(), operation);
                case PUT -> processPut(operation);
                case LIST -> processList(operation);
                case SCAN -> processScan(operation);
                case DELETE -> processDelete(operation);
                case SESSION_RESTART -> processSessionRestart(operation);
                case DELETE_RANGE -> processDeleteRange(operation);
                case OPERATION_NOT_SET -> {
                    log.error("Unsupported operation. operation={}", operation);
                    yield ExecuteResponse.newBuilder()
                            .setStatus(Status.NonRetryableFailure)
                            .setStatusInfo("Unsupported Operation.")
                            .build();
                }
            };
        } catch (Throwable ex) {
            log.error("unexpected error", ex);
            return ExecuteResponse.newBuilder()
                    .setStatus(Status.RetryableFailure)
                    .setStatusInfo(ex.getMessage())
                    .build();
        }
    }


}
