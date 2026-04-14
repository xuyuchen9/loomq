package com.loomq.http;

import com.sun.net.httpserver.HttpExchange;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ReadListener;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpUpgradeHandler;
import jakarta.servlet.http.Part;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Adapts JDK HttpExchange to HttpServletRequest for Javalin compatibility.
 *
 * This is a minimal implementation covering the methods that Javalin actually uses.
 */
class HttpServletRequestAdapter implements HttpServletRequest {

    private final HttpExchange exchange;
    private final String method;
    private final String queryString;
    private final Map<String, String[]> parameterMap;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private byte[] bodyCache;
    private final String contextPath;

    HttpServletRequestAdapter(HttpExchange exchange) {
        this.exchange = exchange;
        this.method = exchange.getRequestMethod();
        this.queryString = exchange.getRequestURI().getQuery();
        this.parameterMap = parseQueryParams(queryString);
        this.contextPath = "";
    }

    private Map<String, String[]> parseQueryParams(String query) {
        Map<String, List<String>> params = new HashMap<>();
        if (query != null && !query.isEmpty()) {
            for (String pair : query.split("&")) {
                int idx = pair.indexOf('=');
                if (idx > 0) {
                    try {
                        String key = java.net.URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                        String value = java.net.URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
                        params.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                    } catch (UnsupportedEncodingException ignored) {}
                } else if (!pair.isEmpty()) {
                    params.computeIfAbsent(pair, k -> new ArrayList<>()).add("");
                }
            }
        }
        Map<String, String[]> result = new HashMap<>();
        params.forEach((k, v) -> result.put(k, v.toArray(new String[0])));
        return result;
    }

    @Override
    public String getMethod() {
        return method;
    }

    @Override
    public String getRequestURI() {
        return exchange.getRequestURI().getPath();
    }

    @Override
    public StringBuffer getRequestURL() {
        String scheme = "http";
        String host = getHeader("Host");
        if (host == null) {
            InetSocketAddress local = exchange.getLocalAddress();
            host = local.getHostString() + ":" + local.getPort();
        }
        return new StringBuffer(scheme + "://" + host + exchange.getRequestURI().getPath());
    }

    @Override
    public String getQueryString() {
        return queryString;
    }

    @Override
    public String getContextPath() {
        return contextPath;
    }

    @Override
    public String getServletPath() {
        return exchange.getRequestURI().getPath();
    }

    @Override
    public String getPathInfo() {
        return null;
    }

    @Override
    public String getPathTranslated() {
        return null;
    }

    @Override
    public String getProtocol() {
        return "HTTP/1.1";
    }

    @Override
    public String getScheme() {
        return "http";
    }

    @Override
    public String getServerName() {
        InetSocketAddress local = exchange.getLocalAddress();
        return local.getHostString();
    }

    @Override
    public int getServerPort() {
        return exchange.getLocalAddress().getPort();
    }

    @Override
    public String getRemoteAddr() {
        InetSocketAddress remote = exchange.getRemoteAddress();
        return remote.getAddress().getHostAddress();
    }

    @Override
    public String getRemoteHost() {
        InetSocketAddress remote = exchange.getRemoteAddress();
        return remote.getHostString();
    }

    @Override
    public Locale getLocale() {
        String acceptLang = getHeader("Accept-Language");
        if (acceptLang != null && !acceptLang.isEmpty()) {
            String lang = acceptLang.split(",")[0].split(";")[0].trim();
            return Locale.forLanguageTag(lang);
        }
        return Locale.getDefault();
    }

    @Override
    public Enumeration<Locale> getLocales() {
        String acceptLang = getHeader("Accept-Language");
        List<Locale> locales = new ArrayList<>();
        if (acceptLang != null) {
            for (String part : acceptLang.split(",")) {
                String lang = part.split(";")[0].trim();
                if (!lang.isEmpty()) {
                    locales.add(Locale.forLanguageTag(lang));
                }
            }
        }
        if (locales.isEmpty()) {
            locales.add(Locale.getDefault());
        }
        return Collections.enumeration(locales);
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public String getContentType() {
        return getHeader("Content-Type");
    }

    @Override
    public int getContentLength() {
        String len = getHeader("Content-Length");
        return len != null ? Integer.parseInt(len) : 0;
    }

    @Override
    public long getContentLengthLong() {
        String len = getHeader("Content-Length");
        return len != null ? Long.parseLong(len) : 0L;
    }

    @Override
    public String getCharacterEncoding() {
        String ct = getContentType();
        if (ct != null) {
            for (String part : ct.split(";")) {
                part = part.trim();
                if (part.toLowerCase().startsWith("charset=")) {
                    return part.substring(8);
                }
            }
        }
        return "UTF-8";
    }

    @Override
    public void setCharacterEncoding(String env) throws UnsupportedEncodingException {
        // Store the encoding for later use
        this.characterEncoding = env;
    }

    private String characterEncoding = "UTF-8";

    @Override
    public ServletInputStream getInputStream() throws IOException {
        if (bodyCache == null) {
            try (InputStream is = exchange.getRequestBody()) {
                bodyCache = is.readAllBytes();
            }
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(bodyCache);
        return new ServletInputStream() {
            @Override
            public int read() throws IOException {
                return bais.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return bais.read(b, off, len);
            }

            @Override
            public boolean isFinished() {
                return bais.available() == 0;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener listener) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(getInputStream(), getCharacterEncoding()));
    }

    @Override
    public String getParameter(String name) {
        String[] values = parameterMap.get(name);
        return values != null && values.length > 0 ? values[0] : null;
    }

    @Override
    public Enumeration<String> getParameterNames() {
        return Collections.enumeration(parameterMap.keySet());
    }

    @Override
    public String[] getParameterValues(String name) {
        return parameterMap.get(name);
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        return parameterMap;
    }

    @Override
    public String getHeader(String name) {
        return exchange.getRequestHeaders().getFirst(name);
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
        List<String> values = exchange.getRequestHeaders().get(name);
        return values != null ? Collections.enumeration(values) : Collections.emptyEnumeration();
    }

    @Override
    public Enumeration<String> getHeaderNames() {
        return Collections.enumeration(exchange.getRequestHeaders().keySet());
    }

    @Override
    public int getIntHeader(String name) {
        String value = getHeader(name);
        return value != null ? Integer.parseInt(value) : -1;
    }

    @Override
    public long getDateHeader(String name) {
        String value = getHeader(name);
        return value != null ? Long.parseLong(value) : -1;
    }

    @Override
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    @Override
    public void removeAttribute(String name) {
        attributes.remove(name);
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        return Collections.enumeration(attributes.keySet());
    }

    @Override
    public HttpSession getSession() {
        return null;
    }

    @Override
    public HttpSession getSession(boolean create) {
        return null;
    }

    @Override
    public String getRequestedSessionId() {
        return null;
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        return false;
    }

    @Override
    public Cookie[] getCookies() {
        String cookieHeader = getHeader("Cookie");
        if (cookieHeader == null || cookieHeader.isEmpty()) {
            return null;
        }
        List<Cookie> cookies = new ArrayList<>();
        for (String pair : cookieHeader.split(";")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String name = pair.substring(0, eq).trim();
                String value = pair.substring(eq + 1).trim();
                cookies.add(new Cookie(name, value));
            }
        }
        return cookies.toArray(new Cookie[0]);
    }

    @Override
    public String getAuthType() {
        return null;
    }

    @Override
    public String getRemoteUser() {
        return null;
    }

    @Override
    public boolean isUserInRole(String role) {
        return false;
    }

    @Override
    public Principal getUserPrincipal() {
        return null;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
        return null;
    }

    @Override
    public String getRealPath(String path) {
        return null;
    }

    @Override
    public int getRemotePort() {
        return exchange.getRemoteAddress().getPort();
    }

    @Override
    public String getLocalName() {
        return exchange.getLocalAddress().getHostString();
    }

    @Override
    public String getLocalAddr() {
        return exchange.getLocalAddress().getAddress().getHostAddress();
    }

    @Override
    public int getLocalPort() {
        return exchange.getLocalAddress().getPort();
    }

    @Override
    public ServletContext getServletContext() {
        return null;
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
        throw new IllegalStateException("Async not supported");
    }

    @Override
    public AsyncContext startAsync(ServletRequest request, ServletResponse response) throws IllegalStateException {
        throw new IllegalStateException("Async not supported");
    }

    @Override
    public boolean isAsyncStarted() {
        return false;
    }

    @Override
    public boolean isAsyncSupported() {
        return false;
    }

    @Override
    public AsyncContext getAsyncContext() {
        return null;
    }

    @Override
    public DispatcherType getDispatcherType() {
        return DispatcherType.REQUEST;
    }

    @Override
    public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
        return false;
    }

    @Override
    public void login(String username, String password) throws ServletException {
    }

    @Override
    public void logout() throws ServletException {
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
        return Collections.emptyList();
    }

    @Override
    public Part getPart(String name) throws IOException, ServletException {
        return null;
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
        return null;
    }

    @Override
    public String changeSessionId() {
        return null;
    }

    public HttpExchange getExchange() {
        return exchange;
    }
}
