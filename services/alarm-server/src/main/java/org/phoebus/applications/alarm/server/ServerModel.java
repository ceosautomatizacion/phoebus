/*******************************************************************************
 * Copyright (c) 2018-2025 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ****************************************************************************** */
package org.phoebus.applications.alarm.server;

import static org.phoebus.applications.alarm.AlarmSystem.logger;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.phoebus.applications.alarm.AlarmSystem;
import org.phoebus.applications.alarm.client.AlarmClientNode;
import org.phoebus.applications.alarm.client.ClientState;
import org.phoebus.applications.alarm.client.IdentificationHelper;
import org.phoebus.applications.alarm.client.KafkaHelper;
import org.phoebus.applications.alarm.messages.AlarmDetail;
import org.phoebus.applications.alarm.messages.AlarmStateMessage;
import org.phoebus.applications.alarm.model.AlarmState;
import org.phoebus.applications.alarm.model.AlarmTreeItem;
import org.phoebus.applications.alarm.model.AlarmTreePath;
import org.phoebus.applications.alarm.model.BasicState;
import org.phoebus.applications.alarm.model.SeverityLevel;
import org.phoebus.applications.alarm.model.json.JsonModelReader;
import org.phoebus.applications.alarm.model.json.JsonModelWriter;
import org.phoebus.applications.alarm.server.actions.InfoPVActionExecutor;

@SuppressWarnings("nls")
class ServerModel {
    private final ConcurrentHashMap<String, ClientState> initial_states;

    private final String config_state_topic, command_topic, talk_topic;
    private final ServerModelListener listener;
    private final AlarmServerNode root;
    private volatile boolean running = true;
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final Thread thread;
    private long last_state_update = 0;
    private long last_annunciation = 0;

    private long last_connection_check = System.currentTimeMillis();
    private boolean connection_lost = false;

    private static final int KAFKA_CLIENT_TIMEOUT = 10;

    public ServerModel(final String kafka_servers, final String config_name,
            final ConcurrentHashMap<String, ClientState> initial_states,
            final ServerModelListener listener,
            final String kafka_properties_file) {
        this.initial_states = initial_states;

        config_state_topic = Objects.requireNonNull(config_name);
        command_topic = config_name + AlarmSystem.COMMAND_TOPIC_SUFFIX;
        talk_topic = config_name + AlarmSystem.TALK_TOPIC_SUFFIX;
        this.listener = Objects.requireNonNull(listener);

        root = new AlarmServerNode(this, null, config_name);

        consumer = KafkaHelper.connectConsumer(Objects.requireNonNull(kafka_servers),
                List.of(config_state_topic, command_topic),
                List.of(config_state_topic),
                kafka_properties_file);
        producer = KafkaHelper.connectProducer(kafka_servers, kafka_properties_file);

        thread = new Thread(this::run, "ServerModel");
        thread.setDaemon(true);
    }

    public void start() {
        thread.start();
        SeverityPVHandler.initialize();
        InfoPVActionExecutor.initialize();

        sendAnnunciatorMessage(root.getPathName(), SeverityLevel.OK, "* Alarm server started. Everything is going to be all right.");
    }

    public AlarmServerNode getRoot() {
        return root;
    }

    private void run() {
        try {
            while (running) {
                checkUpdates();
                final long now = System.currentTimeMillis();
                checkIdle(now);
                checkNag(now);
                checkConnectivity(now);
            }
        } catch (Throwable ex) {
            if (running) {
                logger.log(Level.SEVERE, "Server model error", ex);
            }
        } finally {
            consumer.close();
        }
    }

    private void checkConnectivity(final long now) {
        if (AlarmSystem.connection_check_secs < 0
                || (now - last_connection_check) < AlarmSystem.connection_check_secs * 1000) {
            return;
        }

        boolean connected = false;
        try {
            logger.log(Level.FINE, "Testing Kafka connectitity");
            consumer.listTopics(Duration.ofSeconds(1));
            connected = true;
        } catch (Throwable ex) {
            logger.log(Level.FINE, "No Kafka connectitity", ex);
        }

        if (!connected && !connection_lost) {
            logger.log(Level.WARNING, "Lost Kafka connectitity");
        } else if (connected && connection_lost) {
            logger.log(Level.WARNING, "Regained Kafka connectitity");
            resend(getRoot());
            sendAnnunciatorMessage(root.getPathName(), SeverityLevel.OK, "* Alarm server re-connected");
        }

        connection_lost = !connected;
        last_connection_check = now;
    }

    private void checkUpdates() {
        final ConsumerRecords<String, String> records;
        try {
            records = consumer.poll(Duration.ofMillis(100));
        } catch (Throwable ex) {
            logger.log(Level.WARNING, "Error reading updates from Kafka", ex);
            return;
        }
        for (ConsumerRecord<String, String> record : records) {
            final int sep = record.key().indexOf(':');
            if (sep < 0) {
                logger.log(Level.WARNING, "Invalid key, expecting type:path, got " + record.key());
                continue;
            }

            final String type = record.key().substring(0, sep + 1);
            final String path = record.key().substring(sep + 1);
            if (type.equals(AlarmSystem.COMMAND_PREFIX) || record.topic().equals(command_topic)) {
                final String json = record.value();
                listener.handleCommand(path, json);
            } else if (type.equals(AlarmSystem.CONFIG_PREFIX)) {
                final String node_config = record.value();
                try {
                    if (node_config == null) {
                        final AlarmTreeItem<?> node = deleteNode(path);
                        if (node != null) {
                            stopDeletedPVs(node);
                        }
                    } else {
                        final Object json = JsonModelReader.parseJsonText(node_config);

                        if (JsonModelReader.isConfigDeletion(json)) {
                            continue;
                        }

                        AlarmTreeItem<?> node = findNode(path);

                        final boolean new_node = node == null;
                        if (new_node) {
                            node = findOrCreateNode(path, JsonModelReader.isLeafConfigOrState(json));
                        }

                        if (node instanceof AlarmServerPV && !new_node) {
                            ((AlarmServerPV) node).stop();
                        }

                        JsonModelReader.updateAlarmItemConfig(node, json);

                        if (node instanceof AlarmServerPV) {
                            final AlarmServerPV pv = (AlarmServerPV) node;
                            pv.getParent().maximizeSeverity();
                            pv.start();

                            LocalDateTime enabled_date = pv.getEnabledDate();
                            if (enabled_date != null && enabled_date.isBefore(LocalDateTime.now())) {
                                pv.setEnabled(true);
                            }
                        }
                    }
                } catch (Exception ex) {
                    logger.log(Level.WARNING,
                            "Alarm config update error for path " + path
                            + ", config " + node_config, ex);
                }
            }
        }
    }

    public AlarmTreeItem<?> findNode(final String path) throws Exception {
        final String[] path_elements = AlarmTreePath.splitPath(path);

        if (path_elements.length < 1
                || !root.getName().equals(path_elements[0])) {
            throw new Exception("Invalid path for alarm configuration " + root.getName() + ": " + path);
        }

        AlarmTreeItem<?> node = root;
        for (int i = 1; i < path_elements.length; ++i) {
            final String name = path_elements[i];
            node = node.getChild(name);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    public AlarmServerPV findPV(final String name) {
        return findPV(name, root);
    }

    private AlarmServerPV findPV(final String name, final AlarmTreeItem<?> node) {
        if (node instanceof AlarmServerPV) {
            if (node.getName().equalsIgnoreCase(name)) {
                return (AlarmServerPV) node;
            }
        } else {
            for (AlarmTreeItem<?> child : node.getChildren()) {
                final AlarmServerPV pv = findPV(name, child);
                if (pv != null) {
                    return pv;
                }
            }
        }
        return null;
    }

    private AlarmTreeItem<?> findOrCreateNode(final String path, final boolean is_leaf) throws Exception {
        final String[] path_elements = AlarmTreePath.splitPath(path);

        if (path_elements.length < 1
                || !root.getName().equals(path_elements[0])) {
            throw new Exception("Invalid path for alarm configuration " + root.getName() + ": " + path);
        }

        AlarmClientNode parent = root;
        for (int i = 1; i < path_elements.length; ++i) {
            final String name = path_elements[i];
            final boolean last = i == path_elements.length - 1;
            AlarmTreeItem<?> node = parent.getChild(name);
            if (node == null) {
                if (last && is_leaf) {
                    final AlarmServerPV pv = new AlarmServerPV(this, parent.getPathName(), name, initial_states.remove(path));
                    pv.addToParent(parent);
                    return pv;
                } else {
                    node = new AlarmServerNode(this, parent.getPathName(), name);
                    node.addToParent(parent);
                }
            }
            if (last) {
                return node;
            }
            parent = (AlarmClientNode) node;
        }

        return parent;
    }

    private AlarmTreeItem<?> deleteNode(final String path) throws Exception {
        final AlarmTreeItem<?> node = findNode(path);
        if (node == null) {
            return null;
        }

        node.setActions(Collections.emptyList());

        final AlarmTreeItem<BasicState> parent = node.getParent();
        node.detachFromParent();

        if (parent instanceof AlarmServerNode) {
            ((AlarmServerNode) parent).maximizeSeverity();
        }
        return node;
    }

    private void stopDeletedPVs(final AlarmTreeItem<?> node) {
        if (node instanceof AlarmServerPV) {
            ((AlarmServerPV) node).stop();
            sendStateUpdate(node.getPathName(), null);
        } else {
            for (AlarmTreeItem<?> child : node.getChildren()) {
                stopDeletedPVs(child);
            }
        }
    }

    public void sendStateUpdate(final String path, final BasicState new_state)
    {
        sendStateUpdate(path, new_state, Collections.emptyList());
    }

    public void sendStateUpdate(final String path,
                               final BasicState new_state,
                               final List<AlarmDetail> guidance)
    {
        try
        {
            String json = null;

            if (new_state != null)
            {
                final AlarmStateMessage msg = new AlarmStateMessage();

                msg.setUser(IdentificationHelper.getUser());
                msg.setHost(IdentificationHelper.getHost());
                msg.setGuidance(guidance);
                msg.setSeverity(new_state.getSeverity().name());

                if (new_state instanceof ClientState)
                {
                    final ClientState state = (ClientState) new_state;

                    msg.setMessage(state.message);
                    msg.setValue(state.value);
                    msg.setCurrent_severity(state.current_severity.name());
                    msg.setCurrent_message(state.current_message);

                    if (state.time != null)
                        msg.setInstant(state.time);

                    msg.setLatch(state.latch);
                }

                json = msg.toString();
            }

            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(config_state_topic, AlarmSystem.STATE_PREFIX + path, json);

            producer.send(record);
            last_state_update = System.currentTimeMillis();
        }
        catch (Throwable ex)
        {
            logger.log(Level.WARNING, "Cannot send state update for " + path, ex);
        }
    }

    public void sendConfigUpdate(final String path, final AlarmTreeItem<AlarmState> config) {
        try {
            final String json = config == null ? null : new String(JsonModelWriter.toJsonBytes(config));
            final ProducerRecord<String, String> record = new ProducerRecord<>(config_state_topic, AlarmSystem.CONFIG_PREFIX + path, json);
            producer.send(record);
        } catch (Throwable ex) {
            logger.log(Level.WARNING, "Cannot send config update for " + path, ex);
        }
    }

    public void sendAnnunciatorMessage(final String path, final SeverityLevel severity, final String message) {
        try {
            last_annunciation = System.currentTimeMillis();

            final String json = JsonModelWriter.talkToString(severity, message);
            final ProducerRecord<String, String> record = new ProducerRecord<>(talk_topic, AlarmSystem.TALK_PREFIX + path, json);
            producer.send(record);
        } catch (Throwable ex) {
            logger.log(Level.WARNING, "Cannot send talk message for " + path, ex);
        }
    }

    public void resend(final AlarmTreeItem<?> node) {
        final BasicState state;
        if (node instanceof AlarmServerPV) {
            final AlarmServerPV pv = (AlarmServerPV) node;
            final AlarmState current = pv.getCurrentState();
            state = new ClientState(pv.getState(), current.getSeverity(), current.getMessage());
            sendStateUpdate(pv.getPathName(), state);
        } else {
            state = node.getState();
        }

        logger.log(Level.INFO, "Resend state:" + node.getPathName() + ": " + state);
        sendStateUpdate(node.getPathName(), state);

        for (AlarmTreeItem<?> child : node.getChildren()) {
            resend(child);
        }
    }

    private void checkIdle(final long now) {
        if (now - last_state_update > AlarmSystem.idle_timeout_ms) {
            sendStateUpdate(root.getPathName(), root.getState());
        }
    }

    private void checkNag(final long now) {
        if (AlarmSystem.nag_period_ms > 0
                && now - last_annunciation > AlarmSystem.nag_period_ms) {
            final int active = countAlarmPVs(root);
            if (active == 1) {
                sendAnnunciatorMessage(root.getPathName(), root.getState().severity, "* There is 1 active alarm");
            } else if (active > 1) {
                sendAnnunciatorMessage(root.getPathName(), root.getState().severity, "* There are " + active + " active alarms");
            }
        }
    }

    private int countAlarmPVs(final AlarmTreeItem<?> item) {
        if (item instanceof AlarmServerPV) {
            return ((AlarmServerPV) item).isEnabled() && item.getState().severity.isActive() ? 1 : 0;
        }
        int active = 0;
        for (AlarmTreeItem<?> child : item.getChildren()) {
            if (child.getState().severity.isActive()) {
                active += countAlarmPVs(child);
            }
        }
        return active;
    }

    private void clearActionsAndStopPVs(final AlarmTreeItem<?> node) {
        node.setActions(Collections.emptyList());

        if (node instanceof AlarmServerPV) {
            ((AlarmServerPV) node).stop();
        } else {
            for (AlarmTreeItem<?> child : node.getChildren()) {
                clearActionsAndStopPVs(child);
            }
        }
    }

    public void shutdown() {
        SeverityPVHandler.stop();
        InfoPVActionExecutor.stop();
        running = false;
        consumer.wakeup();
        try {
            thread.join(2000);
        } catch (InterruptedException ex) {
            logger.log(Level.WARNING, "Server model thread doesn't shut down", ex);
        }

        logger.info(thread.getName() + " shut down");

        clearActionsAndStopPVs(root);
        logger.info("Stopped all PVs");

        root.getChildren().clear();
        root.maximizeSeverity();
        logger.info("Cleared configuration for " + root.getName());
    }
}