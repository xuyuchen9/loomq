package com.loomq.http;

import com.sun.net.httpserver.HttpExchange;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Adapts JDK HttpExchange to HttpServletResponse for Javalin compatibility.
 */
class HttpServletResponseAdapter implements HttpServletResponse {

    private final HttpExchange exchange;
    private final Map<String, List<String>> headers = new ConcurrentHashMap<>();
    private int status = 200;
    private String contentType;
    private String characterEncoding = StandardCharsets.UTF_8.name();
    private ByteArrayOutputStream outputStream;
    private PrintWriter writer;
    private boolean committed = false;

    HttpServletResponseAdapter(HttpExchange exchange) {
        this.exchange = exchange;
        this.outputStream = new ByteArrayOutputStream(8192);
    }

    @Override
    public void setStatus(int sc) {
        this.status = sc;
    }

    @Override
    public void setStatus(int sc, String sm) {
        this.status = sc;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void setContentType(String type) {
        this.contentType = type;
        setHeader("Content-Type", type);
        // Extract charset if present
        if (type != null) {
            for (String part : type.split(";")) {
                part = part.trim();
                if (part.toLowerCase().startsWith("charset=")) {
                    this.characterEncoding = part.substring(8);
                    break;
                }
            }
        }
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public void setCharacterEncoding(String charset) {
        this.characterEncoding = charset;
    }

    @Override
    public String getCharacterEncoding() {
        return characterEncoding;
    }

    @Override
    public void setContentLength(int len) {
        setIntHeader("Content-Length", len);
    }

    @Override
    public void setContentLengthLong(long len) {
        setHeader("Content-Length", String.valueOf(len));
    }

    @Override
    public void setHeader(String name, String value) {
        headers.put(name, Collections.singletonList(value));
    }

    @Override
    public void addHeader(String name, String value) {
        headers.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }

    @Override
    public void setIntHeader(String name, int value) {
        setHeader(name, String.valueOf(value));
    }

    @Override
    public void addIntHeader(String name, int value) {
        addHeader(name, String.valueOf(value));
    }

    @Override
    public boolean containsHeader(String name) {
        return headers.containsKey(name);
    }

    @Override
    public String getHeader(String name) {
        List<String> values = headers.get(name);
        return values != null && !values.isEmpty() ? values.get(0) : null;
    }

    @Override
    public Collection<String> getHeaders(String name) {
        return headers.getOrDefault(name, Collections.emptyList());
    }

    @Override
    public Collection<String> getHeaderNames() {
        return headers.keySet();
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        return new ServletOutputStream() {
            @Override
            public void write(int b) throws IOException {
                outputStream.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                outputStream.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setWriteListener(WriteListener listener) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (writer == null) {
            writer = new PrintWriter(new OutputStreamWriter(outputStream, characterEncoding), false);
        }
        return writer;
    }

    @Override
    public void flushBuffer() throws IOException {
        if (writer != null) {
            writer.flush();
        }
        outputStream.flush();
    }

    @Override
    public int getBufferSize() {
        return outputStream.size();
    }

    @Override
    public void setBufferSize(int size) {
        // Ignore, we're using a growing buffer
    }

    @Override
    public void resetBuffer() {
        if (committed) {
            throw new IllegalStateException("Response already committed");
        }
        outputStream = new ByteArrayOutputStream(8192);
        writer = null;
    }

    @Override
    public void reset() {
        resetBuffer();
        headers.clear();
        status = 200;
        contentType = null;
    }

    @Override
    public boolean isCommitted() {
        return committed;
    }

    /**
     * Send the response to the client. This must be called after the servlet is done processing.
     */
    void commit() throws IOException {
        if (committed) {
            return;
        }

        committed = true;

        // Flush writer if used
        if (writer != null) {
            writer.flush();
        }

        byte[] body = outputStream.toByteArray();

        // Set content length if not already set
        if (!containsHeader("Content-Length")) {
            exchange.getResponseHeaders().set("Content-Length", String.valueOf(body.length));
        }

        // Copy headers to exchange
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            for (String value : entry.getValue()) {
                exchange.getResponseHeaders().add(entry.getKey(), value);
            }
        }

        // Send response headers
        exchange.sendResponseHeaders(status, body.length > 0 ? body.length : -1);

        // Write body
        if (body.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        } else {
            exchange.getResponseBody().close();
        }
    }

    @Override
    public void sendError(int sc) throws IOException {
        sendError(sc, null);
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {
        if (committed) {
            throw new IllegalStateException("Response already committed");
        }
        reset();
        status = sc;
        setContentType("application/json");
        String errorMessage = msg != null ? msg : "Error";
        String jsonBody = String.format("{\"error\":\"%s\"}", errorMessage.replace("\"", "\\\""));
        getWriter().write(jsonBody);
        commit();
    }

    @Override
    public void sendRedirect(String location) throws IOException {
        if (committed) {
            throw new IllegalStateException("Response already committed");
        }
        reset();
        status = 302;
        setHeader("Location", location);
        commit();
    }

    @Override
    public String encodeURL(String url) {
        return url;
    }

    @Override
    public String encodeRedirectURL(String url) {
        return url;
    }

    @Override
    public String encodeUrl(String url) {
        return url;
    }

    @Override
    public String encodeRedirectUrl(String url) {
        return url;
    }

    @Override
    public void setDateHeader(String name, long date) {
        setHeader(name, String.valueOf(date));
    }

    @Override
    public void addDateHeader(String name, long date) {
        addHeader(name, String.valueOf(date));
    }

    @Override
    public void addCookie(Cookie cookie) {
        StringBuilder sb = new StringBuilder();
        sb.append(cookie.getName()).append("=").append(cookie.getValue());
        if (cookie.getPath() != null) {
            sb.append("; Path=").append(cookie.getPath());
        }
        if (cookie.getDomain() != null) {
            sb.append("; Domain=").append(cookie.getDomain());
        }
        if (cookie.getMaxAge() > 0) {
            sb.append("; Max-Age=").append(cookie.getMaxAge());
        }
        if (cookie.getSecure()) {
            sb.append("; Secure");
        }
        if (cookie.isHttpOnly()) {
            sb.append("; HttpOnly");
        }
        addHeader("Set-Cookie", sb.toString());
    }

    @Override
    public Locale getLocale() {
        return Locale.getDefault();
    }

    @Override
    public void setLocale(Locale loc) {
        // Ignore
    }
}
