/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.embedded.Connect;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;

/**
 * AsyncEmbeddedEngine.stopSourceTasks() submits the task's stop to taskService the same pool that
 * runs record polling. so a consumer still blocked at shutdown leaves no thread for the stop to run on.
 * Then engine logs "Stopping of the tasks was interrupted, shutting down immediately", the finally block calls
 * taskService.shutdownNow(), and the queued callable is discarded and SourceTask.stop() never runs.
 * The engine reports STOPPED anyway. On PostgreSQL the abandoned task keeps its walsender, so the slot
 * stays active and the next connector for it fails with "is active for PID"
 */
public class AbandonedWalsenderOnBlockedStopIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbandonedWalsenderOnBlockedStopIT.class);

    private static final String SLOT_NAME = "abandoned_walsender_slot";

    private static final long TASK_MANAGEMENT_TIMEOUT_MS = 5_000;

    private static final long ABANDON_WINDOW_MS = 45_000; // keep it greater than DEFAULT_TASK_MANAGEMENT_TIMEOUT_MS (40s)
    private static final long REPLACEMENT_FAILURE_WINDOW_MS = 25_000;
    private static final Duration ENGINE_STOP_WAIT = Duration.ofSeconds(30);
    private static final Duration CONSUMER_WEDGE_WAIT = Duration.ofSeconds(60);

    private final ReentrantLock lock = new ReentrantLock();

    @BeforeEach
    void before() throws SQLException {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();
        dropSlot();
        TestHelper.execute(
                "CREATE SCHEMA zombie;",
                "CREATE TABLE zombie.test (id INT PRIMARY KEY, val VARCHAR(32));",
                "INSERT INTO zombie.test VALUES(1, 'value1');");
    }

    @AfterEach
    void after() throws SQLException {
        TestHelper.execute("ALTER ROLE CURRENT_USER RESET wal_sender_timeout;");
    }

    @Test
    public void walsenderMustNotSurviveEngineShutdown() throws Exception {
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
                .using(config("abandoned-walsender-connector"))
                .notifying((records, committer) -> {
                    lock.lock();
                    lock.unlock();
                })
                .using(getClass().getClassLoader())
                .build();

        SlotState whileBlocked;

        lock.lock();

        try {
            daemon("zombie-engine", engine);

            awaitConsumerWedged();

            LOGGER.info("consumer blocked, slot state now: {}", readSlot());

            daemon("zombie-close", () -> {
                try {
                    engine.close();
                }
                catch (Exception e) {
                    LOGGER.info("engine.close() threw", e);
                }
            });

            Thread.sleep(ABANDON_WINDOW_MS);

            whileBlocked = readSlot();

            LOGGER.info("after the engine gave up stopping the task: {}", whileBlocked);
        }
        finally {
            // release so engine finish its shutdown
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }

        // STOPPED state
        awaitEngineStopped(engine);

        SlotState afterStopped = readSlot();

        LOGGER.info("after the engine reported STOPPED: {}", afterStopped);

        String replacementFailure = startReplacementAndCaptureFailure();

        LOGGER.info("replacement connector outcome: {}", replacementFailure);

        assertThat(afterStopped.active())
                .as("engine reported STOPPED, but slot %s is still held by pid %s", SLOT_NAME, afterStopped.activePid())
                .isFalse();

        // should fail here
        assertThat(replacementFailure)
                .as("the replacement should have been free to take over slot %s", SLOT_NAME)
                .doesNotContain("is active for PID");
    }

    private static void awaitEngineStopped(DebeziumEngine<?> engine) {
        String engineStopped = "Engine has been already shut down.";

        await()
                .atMost(ENGINE_STOP_WAIT)
                .until(() -> closeRejection(engine), engineStopped::equals);
    }

    private static String closeRejection(DebeziumEngine<?> engine) {
        try {
            engine.close();
            return "closed normally";
        }
        catch (Exception e) {
            return e.getMessage();
        }
    }

    private void awaitConsumerWedged() {
        await().atMost(CONSUMER_WEDGE_WAIT).until(lock::hasQueuedThreads);
    }

    private String startReplacementAndCaptureFailure() throws Exception {
        CompletableFuture<String> outcome = new CompletableFuture<>();

        Properties props = config("replacement-connector");

        // go straight to streaming
        props.setProperty(PostgresConnectorConfig.SNAPSHOT_MODE.name(), "no_data");
        props.setProperty(CommonConnectorConfig.TOPIC_PREFIX.name(), "replacement_server");
        props.setProperty(CommonConnectorConfig.MAX_RETRIES_ON_ERROR.name(), "0");
        props.setProperty(CommonConnectorConfig.RETRIABLE_RESTART_WAIT.name(), "1000");

        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> replacement = DebeziumEngine.create(Connect.class)
                .using(props)
                .using((success, message, error) -> outcome.complete(success ? "Hurray!" : getRootCauseMessage(error)))
                .notifying((records, committer) -> committer.markBatchFinished())
                .using(getClass().getClassLoader())
                .build();

        try {
            daemon("replacement-engine", replacement);

            return outcome.get(REPLACEMENT_FAILURE_WINDOW_MS, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            return "replacement engine's CompletionCallback was never invoked within " + REPLACEMENT_FAILURE_WINDOW_MS + " ms";
        }
        finally {
            closeQuietly(replacement);
        }
    }

    private Properties config(String connectorName) {
        Properties props = new Properties();

        props.putAll(TestHelper.defaultConfig().build().asMap());
        props.setProperty("name", connectorName);
        props.setProperty("table.include.list", "zombie.test");
        props.setProperty(PostgresConnectorConfig.MAX_RETRIES.name(), "3");
        props.setProperty(PostgresConnectorConfig.SLOT_NAME.name(), SLOT_NAME);
        props.setProperty("connector.class", PostgresConnector.class.getName());
        props.setProperty(PostgresConnectorConfig.RETRY_DELAY_MS.name(), "1000");
        props.setProperty(PostgresConnectorConfig.PLUGIN_NAME.name(), "pgoutput");
        props.setProperty(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), "false");
        props.setProperty("offset.storage", MemoryOffsetBackingStore.class.getName());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), String.valueOf(TASK_MANAGEMENT_TIMEOUT_MS));

        return props;
    }

    private static void daemon(String name, Runnable body) {
        Thread thread = new Thread(body, name);

        thread.setDaemon(true);

        thread.start();
    }

    private record SlotState(boolean active, Integer activePid) {
    }

    private static SlotState readSlot() throws SQLException {
        AtomicReference<SlotState> result = new AtomicReference<>(new SlotState(false, null));

        try (JdbcConnection connection = TestHelper.create()) {
            connection.query("select active, active_pid from pg_replication_slots where slot_name = '" + SLOT_NAME + "'",
                    rs -> {
                        if (rs.next()) {
                            final int pid = rs.getInt("active_pid");
                            result.set(new SlotState(rs.getBoolean("active"), rs.wasNull() ? null : pid));
                        }
                    });
        }

        return result.get();
    }

    private static void dropSlot() {
        try (JdbcConnection connection = TestHelper.create()) {
            connection.execute("select pg_drop_replication_slot('" + SLOT_NAME + "')");
        }
        catch (Exception ignored) {
        }
    }

    private static void closeQuietly(DebeziumEngine<?> engine) {
        try {
            engine.close();
        }
        catch (Exception e) {
            LOGGER.debug("closing the replacement engine threw", e);
        }
    }
}
