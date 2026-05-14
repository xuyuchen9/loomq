package com.loomq.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

    @Test
    void defaultConfigUsesExpectedWatermarkOrder() {
        ServerConfig config = ServerConfig.defaultConfig();

        assertEquals(524_288, config.writeBufferLowWaterMark());
        assertEquals(1_048_576, config.writeBufferHighWaterMark());
    }

    @Test
    void rejectsInvertedWriteBufferWaterMarks() {
        Properties props = new Properties();
        props.setProperty("netty.write_buffer_low_water_mark", "1048576");
        props.setProperty("netty.write_buffer_high_water_mark", "524288");

        assertThrows(IllegalArgumentException.class, () -> ServerConfig.fromProperties(props));
    }
}
