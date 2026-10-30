package org.phoebus.applications.alarm.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.phoebus.applications.alarm.model.EnabledState;
import java.util.List;
import java.util.Map;
import java.io.Serializable;
import java.util.List;

@JsonInclude(Include.NON_NULL)
public class AlarmMessageUtil implements Serializable {

    @JsonIgnore
    static final ObjectMapper objectStateMapper = new ObjectMapper();
    static {
        objectStateMapper.registerModule(new JavaTimeModule());
        objectStateMapper.addMixIn(AlarmStateMessage.class, AlarmStateJsonMessage.class);
    }

    @JsonIgnore
    static final ObjectMapper objectConfigMapper = new ObjectMapper();
    static {
        SimpleModule simple_module = new SimpleModule();
        simple_module.addSerializer(new EnabledSerializer());

        objectConfigMapper.registerModule(new JavaTimeModule());
        objectConfigMapper.registerModule(simple_module);
        objectConfigMapper.addMixIn(AlarmMessageUtil.class, AlarmConfigJsonMessage.class);
    }

    @JsonIgnore
    static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.registerModule(new JavaTimeModule());
    }

    private static class AlarmStateJsonMessage {
    private String severity;
    private String message;
    private String value;
    private Map<String, String> time;
    private String current_severity;
    private String current_message;
    private String mode;
    private boolean notify;
    private boolean latch;
    private String config;
    private String pv;
    private String user;
    private String host;
    private List<AlarmDetail> guidance;
}

    private static class AlarmConfigJsonMessage {
        @JsonIgnore
        private EnabledState enabled;
        @JsonIgnore
        private boolean annunciating;
        @JsonIgnore
        private int delay;
        @JsonIgnore
        private int count;
        @JsonIgnore
        private String filter;
        @JsonIgnore
        private List<AlarmDetail> guidance;
        @JsonIgnore
        private List<AlarmDetail> displays;
        @JsonIgnore
        private List<AlarmDetail> commands;
        @JsonIgnore
        private List<AlarmDetail> actions;
    }
}