package com.loomq.http.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

/**
 * Shared JSON codec for the Netty HTTP stack.
 *
 * The project uses Instant-heavy request/response payloads, so we keep a single
 * ObjectMapper with Java time support and relaxed unknown-field handling.
 */
public final class JsonCodec {

    private static final JsonCodec INSTANCE = new JsonCodec();

    private final ObjectMapper mapper;
    private final ObjectWriter writer;

    private JsonCodec() {
        this.mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        this.writer = mapper.writer();
    }

    public static JsonCodec instance() {
        return INSTANCE;
    }

    public <T> T read(byte[] body, Class<T> type) throws IOException {
        return mapper.readValue(body, type);
    }

    public byte[] writeBytes(Object value) throws JsonProcessingException {
        return writer.writeValueAsBytes(value);
    }

    public String writeString(Object value) throws JsonProcessingException {
        return writer.writeValueAsString(value);
    }

    public ObjectMapper mapper() {
        return mapper;
    }
}
