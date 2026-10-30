package org.phoebus.applications.alarm.messages;

import static org.phoebus.applications.alarm.AlarmSystem.logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.phoebus.util.time.TimestampFormats;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;

@JsonInclude(Include.NON_NULL)
public class AlarmStateMessage {

    private static DateTimeFormatter formatter = TimestampFormats.MILLI_FORMAT;

    private String user;
    private String host;

    private String severity;
    private String message;
    private String value;
    private Map<String, String> time;
    private String current_severity;
    private String current_message;
    private String mode;
    private boolean notify = true;

    private List<AlarmDetail> guidance;

    // para log / metadata
    private Instant message_time;
    private boolean latch;

    private String config;
    private String pv;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public List<AlarmDetail> getGuidance() {
        return guidance;
    }

    public void setGuidance(List<AlarmDetail> guidance) {
        this.guidance = guidance;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getPv() {
        return pv;
    }

    public void setPv(String pv) {
        this.pv = pv;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public boolean isLatch() {
        return latch;
    }

    public void setLatch(boolean latch) {
        this.latch = latch;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Map<String, String> getTime() {
        return time;
    }

    public void setTime(Map<String, String> time) {
        this.time = time;
    }

    public String getCurrent_severity() {
        return current_severity;
    }

    public void setCurrent_severity(String current_severity) {
        this.current_severity = current_severity;
    }

    public String getCurrent_message() {
        return current_message;
    }

    public void setCurrent_message(String current_message) {
        this.current_message = current_message;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public boolean getNotify() {
        return notify;
    }

    public void setNotify(boolean notify) {
        this.notify = notify;
    }

    public Instant getMessage_time() {
        return message_time;
    }

    public void setMessage_time(Instant message_time) {
        this.message_time = message_time;
    }

    @JsonIgnore
    public Instant getInstant() {
        return Instant.ofEpochSecond(
            Long.parseLong(time.get("seconds")),
            Long.parseLong(time.get("nano"))
        );
    }

    @JsonIgnore
    public void setInstant(Instant instant) {
        this.time = new HashMap<>();
        this.time.put("seconds", String.valueOf(instant.getEpochSecond()));
        this.time.put("nano", String.valueOf(instant.getNano()));
    }

    @JsonIgnore
    public boolean isLeaf() {
        return value != null
            && message != null
            && time != null
            && current_severity != null
            && current_message != null;
    }

    @JsonIgnore
    public Map<String, String> sourceMap() {
        Map<String, String> map = new HashMap<>();
        map.put("config", getConfig());
        map.put("pv", getPv());
        map.put("severity", getSeverity());
        map.put("latch", Boolean.toString(isLatch()));
        map.put("message", getMessage());
        map.put("value", getValue());
        map.put("time", formatter.withZone(ZoneId.of("UTC")).format(getInstant()));
        map.put("message_time", formatter.withZone(ZoneId.of("UTC")).format(getMessage_time()));
        map.put("current_severity", getCurrent_severity());
        map.put("current_message", getCurrent_message());
        map.put("mode", getMode());
        map.put("notify", Boolean.toString(getNotify()));
        return map;
    }

    @Override
    public String toString() {
        try {
            return AlarmMessageUtil.objectStateMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.log(Level.WARNING, "failed to parse the alarm state message ", e);
        }
        return "";
    }
}