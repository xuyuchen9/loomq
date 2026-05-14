package com.loomq.http.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.loomq.domain.intent.PrecisionTier;
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
        SimpleModule precisionTierModule = new SimpleModule();
        precisionTierModule.addSerializer(PrecisionTier.class, new JsonSerializer<>() {
            @Override
            public void serialize(PrecisionTier value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
                gen.writeString(value != null ? value.toJson() : null);
            }
        });
        precisionTierModule.addDeserializer(PrecisionTier.class, new JsonDeserializer<>() {
            @Override
            public PrecisionTier deserialize(JsonParser p, com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws IOException {
                if (p.currentToken() == JsonToken.VALUE_NULL) {
                    return PrecisionTier.fromString(null);
                }
                return PrecisionTier.fromString(p.getValueAsString());
            }
        });
        this.mapper.registerModule(precisionTierModule);
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
