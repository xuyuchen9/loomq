package com.loomq.cli;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class LoomqCliTest {

    private MockWebServer mockServer;
    private LoomqCli cli;

    @BeforeEach
    void setUp() throws Exception {
        mockServer = new MockWebServer();
        mockServer.start();
        cli = new LoomqCli("http://localhost:" + mockServer.getPort());
    }

    @AfterEach
    void tearDown() throws Exception {
        mockServer.shutdown();
    }

    @Nested
    @DisplayName("命令路由")
    class CommandRouting {

        @Test
        @DisplayName("help 不应该抛异常")
        void helpCommand() {
            assertDoesNotThrow(() -> cli.executeCommand("help"));
        }

        @Test
        @DisplayName("未知命令应不抛异常")
        void unknownCommand() {
            assertDoesNotThrow(() -> cli.executeCommand("foobar"));
        }

        @Test
        @DisplayName("exit 应正常返回")
        void exitCommand() {
            assertDoesNotThrow(() -> cli.executeCommand("exit"));
        }
    }

    @Nested
    @DisplayName("HTTP 命令")
    class HttpCommands {

        @Test
        @DisplayName("health 命令应能处理服务器响应")
        void healthCommand() {
            mockServer.enqueue(new MockResponse()
                .setBody("{\"status\":\"UP\",\"narrative\":\"All systems operational\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));

            assertDoesNotThrow(() -> cli.executeCommand("health"));
        }

        @Test
        @DisplayName("get 命令应能获取 intent")
        void getCommand() {
            mockServer.enqueue(new MockResponse()
                .setBody("{\"intentId\":\"intent_test\",\"status\":\"SCHEDULED\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));

            assertDoesNotThrow(() -> cli.executeCommand("get intent_test"));
        }

        @Test
        @DisplayName("get 命令不带参数不应抛异常")
        void getCommandNoArgs() {
            assertDoesNotThrow(() -> cli.executeCommand("get"));
        }

        @Test
        @DisplayName("list 命令应能列表查询")
        void listCommand() {
            mockServer.enqueue(new MockResponse()
                .setBody("{\"intents\":[],\"total\":0,\"offset\":0,\"limit\":50}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));

            assertDoesNotThrow(() -> cli.executeCommand("list --status SCHEDULED"));
        }

        @Test
        @DisplayName("chronoscope 命令应能处理响应")
        void chronoscopeCommand() {
            mockServer.enqueue(new MockResponse()
                .setBody("{\"tiers\":{},\"borrow\":{},\"permitTiming\":{},\"timestamp\":\"2025-01-01T00:00:00Z\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));

            assertDoesNotThrow(() -> cli.executeCommand("chronoscope"));
        }

        @Test
        @DisplayName("dead-letters 命令应能处理响应")
        void deadLettersCommand() {
            mockServer.enqueue(new MockResponse()
                .setBody("{\"intents\":[],\"total\":0,\"offset\":0,\"limit\":50}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));

            assertDoesNotThrow(() -> cli.executeCommand("dead-letters"));
        }
    }
}
