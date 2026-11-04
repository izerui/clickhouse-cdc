package com.github.izerui.health;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HealthServerTest {

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void livenessAlwaysOkReadinessFollowsJob() throws Exception {
        try (HealthServer server = new HealthServer(0)) {
            server.start();
            int port = server.getPort();
            assertEquals(200, get(port, "/healthz").statusCode());

            HttpResponse<String> ready = get(port, "/readyz");
            assertEquals(503, ready.statusCode());
            assertEquals("STARTING", ready.body().trim());

            server.setPhase("INITIALIZING_TABLES");
            assertEquals("INITIALIZING_TABLES", get(port, "/status").body().trim());
            assertEquals(503, get(port, "/readyz").statusCode());
        }
    }
}
