package dev.resteasy.jetty.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.client.CompletionStageRxInvoker;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import org.apache.http.entity.ContentType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpHeaderValue;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.engines.AsyncClientHttpEngine;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.resteasy.jetty.client.engine.JettyClientEngine;

public class JettyClientEngineTest {
    Server server = new Server(0);
    Client client;

    @AfterEach
    public void stop() throws Exception {
        if (client != null) {
            client.close();
        }
        server.stop();
    }

    private Client client() throws Exception {
        if (!server.isStarted()) {
            server.start();
        }
        if (client == null) {
            client = ClientBuilder.newClient();
        }
        return client;
    }

    @Test
    public void clientCheck() throws Exception {
        Assertions.assertInstanceOf(JettyClientEngine.class, ((ResteasyClient) client()).httpEngine());
    }

    @Test
    public void testSimple() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {

                if (request.getHeaders().get("User-Agent").contains("Apache")) {
                    response.setStatus(503);
                } else if (!"abracadabra".equals(request.getHeaders().get("Password"))) {
                    response.setStatus(403);
                } else {
                    response.setStatus(200);
                    Content.Sink.write(response, true, "Success", callback);
                }

                return true;
            }
        });

        final Response response = client().target(baseUri()).request()
                .header("Password", "abracadabra")
                .get();

        assertEquals(200, response.getStatus());
        assertEquals("Success", response.readEntity(String.class));
    }

    @Test
    public void testSimpleResponseRx() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {

                if (request.getHeaders().get("User-Agent").contains("Apache")) {
                    response.setStatus(503);
                } else if (!"abracadabra".equals(request.getHeaders().get("Password"))) {
                    response.setStatus(403);
                } else {
                    response.setStatus(200);
                    response.getHeaders().put(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
                    Content.Sink.write(response, true, "Success", callback);
                }
                return true;
            }
        });

        final CompletionStage<Response> cs = client().target(baseUri()).request()
                .header("Password", "abracadabra").rx(CompletionStageRxInvoker.class)
                .get();

        Response response = cs.toCompletableFuture().get();
        assertEquals(200, response.getStatus());
        assertEquals("Success", response.readEntity(String.class));
    }

    @Test
    public void testSimpleStringRx() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {

                if (request.getHeaders().get("User-Agent").contains("Apache")) {
                    response.setStatus(503);
                } else if (!"abracadabra".equals(request.getHeaders().get("Password"))) {
                    response.setStatus(403);
                } else {
                    response.setStatus(200);
                    response.getHeaders().put(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
                    Content.Sink.write(response, true, "Success", callback);
                }
                return true;
            }
        });

        final CompletionStage<String> cs = client().target(baseUri()).request()
                .header("Password", "abracadabra").rx(CompletionStageRxInvoker.class)
                .get(String.class);

        String response = cs.toCompletableFuture().get();
        assertEquals("Success", response);
    }

    @Test
    public void testBigly() throws Exception {
        server.setHandler(new EchoHandler());
        final byte[] valuableData = randomAlpha().getBytes(StandardCharsets.UTF_8);
        final Response response = client().target(baseUri()).request()
                .post(Entity.entity(valuableData, MediaType.APPLICATION_OCTET_STREAM_TYPE));

        assertEquals(200, response.getStatus());
        assertArrayEquals(valuableData, response.readEntity(byte[].class));
    }

    @Test
    public void testFutureResponse() throws Exception {
        server.setHandler(new EchoHandler());
        final String valuableData = randomAlpha();
        final Future<Response> response = client().target(baseUri()).request()
                .buildPost(Entity.entity(valuableData, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                .submit();

        final Response resp = response.get(10, TimeUnit.SECONDS);
        assertEquals(200, resp.getStatus());
        assertEquals(valuableData, resp.readEntity(String.class));
    }

    @Test
    public void testFutureString() throws Exception {
        server.setHandler(new EchoHandler());
        final String valuableData = randomAlpha();
        final Future<String> response = client().target(baseUri()).request()
                .buildPost(Entity.entity(valuableData, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                .submit(String.class);

        final String result = response.get(10, TimeUnit.SECONDS);
        assertEquals(valuableData.length(), result.length());
        assertEquals(valuableData, result);
    }

    private String randomAlpha() {
        final StringBuilder builder = new StringBuilder();
        final Random r = new Random();
        for (int i = 0; i < 20 * 1024 * 1024; i++) {
            builder.append((char) ('a' + (char) r.nextInt('z' - 'a')));
            if (i % 100 == 0)
                builder.append('\n');
        }
        return builder.toString();
    }

    @Test
    public void testTimeout() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    callback.failed(e);
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return true;
            }
        });

        try {
            client().target(baseUri()).request()
                    .property(JettyClientEngine.REQUEST_TIMEOUT_MS, Duration.ofMillis(500))
                    .get();
            fail();
        } catch (ProcessingException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                response.setStatus(200);
                Content.Sink.write(response, true, "Success", callback);
                return true;
            }
        });

        try {
            client().target(baseUri()).request()
                    .property(JettyClientEngine.REQUEST_TIMEOUT_MS, Duration.ofMillis(2000))
                    .property(JettyClientEngine.IDLE_TIMEOUT_MS, Duration.ofMillis(500))
                    .get();
            fail();
        } catch (ProcessingException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }

        final Response response = client().target(baseUri()).request()
                .property(JettyClientEngine.REQUEST_TIMEOUT_MS, Duration.ofMillis(2000))
                .property(JettyClientEngine.IDLE_TIMEOUT_MS, Duration.ofMillis(1500))
                .get();

        assertEquals(200, response.getStatus());
        assertEquals("Success", response.readEntity(String.class));

    }

    /**
     * If the async executor is shut down while a request is in flight, the completion task
     * submitted from {@code onHeaders} is rejected. Jetty treats the exchange as successful and
     * swallows that {@link java.util.concurrent.RejectedExecutionException}, so the response
     * future was previously left forever incomplete and the synchronous caller hung on the
     * engine's one-hour {@code future.get}. The engine must instead fail the future, so the call
     * returns promptly with an error.
     */
    @Test
    public void testExecutorShutdownWhileInFlightDoesNotHang() throws Exception {
        final ExecutorService delegate = Executors.newSingleThreadExecutor();
        final RejectionRecordingExecutor executor = new RejectionRecordingExecutor(delegate);

        // The handler shuts the executor down *before* writing any response bytes. Because this
        // runs only after the request has been fully sent and received, the sole remaining executor
        // interaction is the onHeaders completion submit on the client side, which is therefore
        // guaranteed to be rejected -- no race, and the pre-send body path (GET has no entity) is
        // never involved.
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                executor.shutdownNow();
                response.setStatus(200);
                Content.Sink.write(response, true, "Success", callback);
                return true;
            }
        });
        if (!server.isStarted()) {
            server.start();
        }
        client = clientWithExecutor(executor);

        assertTimeoutPreemptively(Duration.ofSeconds(15),
                () -> assertThrows(ProcessingException.class, () -> client.target(baseUri()).request().get(String.class)),
                "synchronous call must fail fast, not hang on the engine's one-hour future.get");
        // Prove the failure was produced by the guarded reject-at-onHeaders path, not some other
        // fast failure, so the test cannot pass for the wrong reason.
        assertTrue(executor.rejectedAfterShutdown(),
                "expected the onHeaders completion submit to be rejected by the shut-down executor");
    }

    /**
     * The empty-body variant of {@link #testExecutorShutdownWhileInFlightDoesNotHang}: the response
     * carries no entity, so there is never a body read that a request timeout or idle timeout could
     * interrupt. The exchange completes cleanly from Jetty's point of view the instant the (empty)
     * response arrives, so the swallowed rejection can only be caught by the engine's own guard --
     * no timeout can rescue this shape. The call must still fail fast rather than hang.
     */
    @Test
    public void testExecutorShutdownWithEmptyBodyDoesNotHang() throws Exception {
        final ExecutorService delegate = Executors.newSingleThreadExecutor();
        final RejectionRecordingExecutor executor = new RejectionRecordingExecutor(delegate);

        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                executor.shutdownNow();
                // 204 with no content: the response has no entity body at all.
                response.setStatus(204);
                callback.succeeded();
                return true;
            }
        });
        if (!server.isStarted()) {
            server.start();
        }
        client = clientWithExecutor(executor);

        assertTimeoutPreemptively(Duration.ofSeconds(15),
                () -> assertThrows(ProcessingException.class, () -> client.target(baseUri()).request().get()),
                "empty-body call must fail fast; no timeout can rescue a body-less response");
        assertTrue(executor.rejectedAfterShutdown(),
                "expected the onHeaders completion submit to be rejected by the shut-down executor");
    }

    /**
     * A request with an entity hands the body-writing task to the async executor before the request
     * is sent. If that executor is already shut down, the submission is rejected on the calling
     * thread; the engine must translate that into a {@link ProcessingException} and complete the
     * future exceptionally rather than letting a raw {@code RejectedExecutionException} escape and
     * leaving the future dangling.
     */
    @Test
    public void testEntityRequestFailsFastWhenExecutorShutDown() throws Exception {
        server.setHandler(new EchoHandler());
        if (!server.isStarted()) {
            server.start();
        }

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.shutdownNow();
        client = clientWithExecutor(executor);

        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> assertThrows(ProcessingException.class,
                () -> client.target(baseUri()).request()
                        .post(Entity.entity("body", MediaType.TEXT_PLAIN_TYPE), String.class)));
    }

    /**
     * A server can reject a request with {@code Connection: close} before it reads the chunked body. The response
     * that already arrived must be the result of the call, although the body write then fails. Jetty stops the body
     * of a 401 itself, in its authentication handler. A 500 has no protocol handler, so the write fails on the closed
     * connection.
     * <p>
     * The single-thread executor makes the order deterministic. The body writer holds the only thread until the
     * server closed the connection and {@code onHeaders} queued the completion task. Only then does it write until a
     * write fails.
     * <p>
     * The test does not read the body. Over HTTP/1.1, a request failure also fails a response that the application did
     * not read to its end, so Jetty can discard the body of the 500.
     * {@link #earlyChallengeKeepsBodyAfterBodyWriteFailure()} covers the body of a 401.
     */
    @ParameterizedTest
    @ValueSource(ints = { 401, 500 })
    public void earlyResponseWinsOverBodyWriteFailure(final int status) throws Exception {
        final CountDownLatch serverClosed = startEarlyRejectingServer(status, "nope!");
        final SignalingExecutor executor = new SignalingExecutor(Executors.newSingleThreadExecutor());
        client = clientWithExecutor(executor);
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final Response response = client.target(baseUri()).request()
                        .post(Entity.entity(writeUntilFailure(serverClosed, executor.completionQueued),
                                MediaType.TEXT_PLAIN_TYPE));
                assertEquals(status, response.getStatus());
                response.close();
            });
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Jetty buffers a 401 in its authentication handler, so the whole response arrived before the body write
     * fails. The body must stay readable. The handler forwards the 401 as a success, so this test does not cover a
     * request-only failure that arrives with the response. The listener unit test covers that case.
     */
    @Test
    public void earlyChallengeKeepsBodyAfterBodyWriteFailure() throws Exception {
        final CountDownLatch serverClosed = startEarlyRejectingServer(401, "nope!");
        final SignalingExecutor executor = new SignalingExecutor(Executors.newSingleThreadExecutor());
        client = clientWithExecutor(executor);
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final Response response = client.target(baseUri()).request()
                        .post(Entity.entity(writeUntilFailure(serverClosed, executor.completionQueued),
                                MediaType.TEXT_PLAIN_TYPE));
                assertEquals(401, response.getStatus());
                assertEquals("nope!", response.readEntity(String.class));
            });
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * The asynchronous variant of {@link #earlyResponseWinsOverBodyWriteFailure}: the callback gets exactly one
     * outcome, and that outcome is the response. RESTEasy buffers the entity of an asynchronous response, so the
     * response has an empty body, which Jetty cannot discard.
     */
    @ParameterizedTest
    @ValueSource(ints = { 401, 500 })
    public void earlyResponseWinsOverBodyWriteFailureAsync(final int status) throws Exception {
        final CountDownLatch serverClosed = startEarlyRejectingServer(status, "");
        final SignalingExecutor executor = new SignalingExecutor(Executors.newSingleThreadExecutor());
        client = clientWithExecutor(executor);
        final List<String> outcomes = new CopyOnWriteArrayList<>();
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final Future<Response> future = client.target(baseUri()).request().async()
                        .post(Entity.entity(writeUntilFailure(serverClosed, executor.completionQueued),
                                MediaType.TEXT_PLAIN_TYPE), new InvocationCallback<Response>() {
                                    @Override
                                    public void completed(final Response response) {
                                        outcomes.add("completed " + response.getStatus());
                                        response.close();
                                    }

                                    @Override
                                    public void failed(final Throwable throwable) {
                                        outcomes.add("failed " + throwable);
                                    }
                                });
                try {
                    future.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    // The outcomes assertion reports the failure.
                }
                // Both callback paths run on the single executor thread, so a task queued now runs after them.
                executor.submit(() -> {
                }).get(10, TimeUnit.SECONDS);
            });
        } finally {
            executor.shutdownNow();
        }
        assertEquals(List.of("completed " + status), outcomes);
    }

    /**
     * Jetty fails the response of a 401 without a {@code WWW-Authenticate} challenge, after it delivered the whole
     * body. The call must return the 401 with its body.
     * <p>
     * A gate task holds the single executor thread until the exchange is complete. Jetty notifies the failure to
     * every listener before it notifies completion, so the completion task always runs after the failure.
     */
    @Test
    public void unchallengedUnauthorizedKeepsBody() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) {
                response.setStatus(401);
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, MediaType.TEXT_PLAIN);
                response.getHeaders().put(HttpHeader.CONTENT_LENGTH, 5);
                Content.Sink.write(response, true, "nope!", callback);
                return true;
            }
        });
        server.start();
        final CountDownLatch exchangeComplete = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        client = clientWithExecutor(executor, new HttpClient() {
            @Override
            public org.eclipse.jetty.client.Request newRequest(final URI uri) {
                return super.newRequest(uri).onComplete(result -> exchangeComplete.countDown());
            }
        });
        try {
            executor.execute(() -> await(exchangeComplete));
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final Response response = client.target(baseUri()).request().get();
                assertEquals(401, response.getStatus());
                assertEquals("nope!", response.readEntity(String.class));
            });
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * If the entity writer fails, the server must not get a well-formed request whose body is cut short, and the
     * caller must get the failure of the writer.
     */
    @Test
    public void bodyWriterFailureDoesNotSendTruncatedBody() throws Exception {
        final CompletableFuture<String> serverRead = new CompletableFuture<>();
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) {
                try {
                    serverRead.complete("read " + Content.Source.asString(request));
                } catch (Throwable t) {
                    serverRead.complete("failed");
                }
                response.setStatus(400);
                callback.succeeded();
                return true;
            }
        });
        server.start();
        client = ClientBuilder.newClient();

        final StreamingOutput failingWriter = output -> {
            output.write("partial".getBytes(StandardCharsets.UTF_8));
            output.flush();
            throw new IOException("application failure");
        };
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            final ProcessingException e = assertThrows(ProcessingException.class,
                    () -> client.target(baseUri()).request().post(Entity.entity(failingWriter, MediaType.TEXT_PLAIN_TYPE)));
            assertEquals("application failure", e.getCause().getMessage());
            assertEquals("failed", serverRead.get(10, TimeUnit.SECONDS));
        });
    }

    /**
     * The entity writer can fail before the engine sends the request, for example when no message body writer fits.
     * The call must fail promptly with the failure of the writer. The inline executor runs the body writer on the
     * calling thread, before the request is sent.
     */
    @Test
    public void bodyWriterFailureBeforeSendFailsPromptly() throws Exception {
        server.setHandler(new ReadBodyHandler());
        server.start();
        client = clientWithExecutor(new InlineExecutor());

        final StreamingOutput failingWriter = output -> {
            throw new IOException("application failure");
        };
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            final ProcessingException e = assertThrows(ProcessingException.class,
                    () -> client.target(baseUri()).request().post(Entity.entity(failingWriter, MediaType.TEXT_PLAIN_TYPE)));
            assertEquals("application failure", e.getCause().getMessage());
        });
    }

    /**
     * An {@link Error} from the entity writer must fail the call promptly as a {@link ProcessingException}, both
     * before and after the engine sends the request.
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void bodyWriterErrorFailsPromptly(final boolean beforeSend) throws Exception {
        server.setHandler(new ReadBodyHandler());
        server.start();
        final ExecutorService executor = beforeSend ? new InlineExecutor() : Executors.newSingleThreadExecutor();
        client = clientWithExecutor(executor);

        final StreamingOutput failingWriter = output -> {
            if (!beforeSend) {
                // The flush returns only after Jetty reads the content, so the request is sent.
                output.write('x');
                output.flush();
            }
            throw new NoClassDefFoundError("missing");
        };
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final ProcessingException e = assertThrows(ProcessingException.class,
                        () -> client.target(baseUri()).request()
                                .post(Entity.entity(failingWriter, MediaType.TEXT_PLAIN_TYPE)));
                assertTrue(hasCause(e, NoClassDefFoundError.class), () -> "unexpected cause chain: " + e);
            });
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * A cancel of the future gives the callback exactly one outcome, a {@link ProcessingException}, as the RESTEasy
     * Apache engine does.
     */
    @Test
    public void cancelNotifiesCallbackOnce() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) {
                await(release);
                response.setStatus(200);
                callback.succeeded();
                return true;
            }
        });
        server.start();
        final CountDownLatch exchangeComplete = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        client = clientWithExecutor(executor, new HttpClient() {
            @Override
            public org.eclipse.jetty.client.Request newRequest(final URI uri) {
                return super.newRequest(uri).onComplete(result -> exchangeComplete.countDown());
            }
        });
        final List<String> outcomes = new CopyOnWriteArrayList<>();
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final ClientInvocation invocation = (ClientInvocation) client.target(baseUri()).request().buildGet();
                final Future<ClientResponse> future = ((AsyncClientHttpEngine) ((ResteasyClient) client).httpEngine())
                        .submit(invocation, false, new InvocationCallback<ClientResponse>() {
                            @Override
                            public void completed(final ClientResponse response) {
                                outcomes.add("completed " + response.getStatus());
                            }

                            @Override
                            public void failed(final Throwable throwable) {
                                outcomes.add("failed " + throwable.getClass().getSimpleName());
                            }
                        }, null);
                assertTrue(future.cancel(true));
                await(exchangeComplete);
                executor.submit(() -> {
                }).get(10, TimeUnit.SECONDS);
            });
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
        assertEquals(List.of("failed ProcessingException"), outcomes);
    }

    /**
     * Jetty buffers a chunked 401 without a {@code WWW-Authenticate} challenge, delivers it as one last chunk, and
     * then fails the response. The call must return the 401 with its body.
     * <p>
     * A gate task holds the single executor thread until the exchange is complete. Jetty notifies the failure to
     * every listener before it notifies completion, so the completion task always runs after the failure.
     */
    @Test
    public void chunkedUnchallengedUnauthorizedKeepsBody() throws Exception {
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                response.setStatus(401);
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, MediaType.TEXT_PLAIN);
                final Callback.Completable first = new Callback.Completable();
                Content.Sink.write(response, false, "no", first);
                first.get(10, TimeUnit.SECONDS);
                Content.Sink.write(response, true, "pe!", callback);
                return true;
            }
        });
        server.start();
        final CountDownLatch exchangeComplete = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        client = clientWithExecutor(executor, new HttpClient() {
            @Override
            public org.eclipse.jetty.client.Request newRequest(final URI uri) {
                return super.newRequest(uri).onComplete(result -> exchangeComplete.countDown());
            }
        });
        try {
            executor.execute(() -> await(exchangeComplete));
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                final Response response = client.target(baseUri()).request().get();
                assertEquals(401, response.getStatus());
                assertEquals(null, response.getHeaderString(HttpHeaders.CONTENT_LENGTH));
                assertEquals("nope!", response.readEntity(String.class));
            });
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * The read timeout of the engine is in the unit that the constructor gets.
     */
    @Test
    public void readTimeoutUsesItsUnit() throws Exception {
        server.setHandler(new EchoHandler());
        server.start();
        final List<org.eclipse.jetty.client.Request> requests = new CopyOnWriteArrayList<>();
        final HttpClient httpClient = new HttpClient() {
            @Override
            public org.eclipse.jetty.client.Request newRequest(final URI uri) {
                final org.eclipse.jetty.client.Request request = super.newRequest(uri);
                requests.add(request);
                return request;
            }
        };
        client = new ResteasyClientBuilderImpl()
                .httpEngine(new JettyClientEngine(httpClient, 1, TimeUnit.MINUTES))
                .build();

        assertTimeoutPreemptively(Duration.ofSeconds(15),
                () -> assertEquals(200, client.target(baseUri()).request().get().getStatus()));
        assertEquals(List.of(TimeUnit.MINUTES.toMillis(1)), requests.stream().map(r -> r.getTimeout()).toList());
    }

    private static boolean hasCause(final Throwable throwable, final Class<? extends Throwable> type) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Starts a server that reads one byte of the request body, answers {@code status} and {@code body} with
     * {@code Connection: close}, and closes the connection. A 401 carries a challenge, so that the authentication handler of
     * Jetty forwards it
     * as a success.
     *
     * @return a latch that opens when the server closed the connection
     */
    private CountDownLatch startEarlyRejectingServer(final int status, final String body) throws Exception {
        final CountDownLatch serverClosed = new CountDownLatch(1);
        server.getConnectors()[0].addBean(new Connection.Listener() {
            @Override
            public void onClosed(final Connection connection) {
                serverClosed.countDown();
            }
        });
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws IOException {
                // Read the byte that the client sent first, so that the close is a FIN and not a reset that could
                // discard the response on the client.
                Content.Source.asInputStream(request).read();
                response.setStatus(status);
                response.getHeaders().put(HttpHeader.CONNECTION, HttpHeaderValue.CLOSE.asString());
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, MediaType.TEXT_PLAIN);
                response.getHeaders().put(HttpHeader.CONTENT_LENGTH, body.length());
                if (status == 401) {
                    response.getHeaders().put(HttpHeader.WWW_AUTHENTICATE, "Bearer realm=\"test\"");
                }
                // With only Connection: close, the server shuts down its output and continues to read, so the
                // body writes of the client never fail.
                final EndPoint endPoint = request.getConnectionMetaData().getConnection().getEndPoint();
                Content.Sink.write(response, true, body, Callback.from(() -> {
                    endPoint.close();
                    callback.succeeded();
                }, callback::failed));
                return true;
            }
        });
        server.start();
        return serverClosed;
    }

    /**
     * An entity that sends one byte, waits for {@code latches}, and then writes until a write fails.
     */
    private static StreamingOutput writeUntilFailure(final CountDownLatch... latches) {
        return output -> {
            output.write('x');
            output.flush();
            for (CountDownLatch latch : latches) {
                await(latch);
            }
            final byte[] chunk = new byte[8 * 1024];
            while (true) {
                output.write(chunk);
                output.flush();
            }
        };
    }

    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch did not open");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static ResteasyClient clientWithExecutor(final ExecutorService executor) {
        return clientWithExecutor(executor, new HttpClient());
    }

    private static ResteasyClient clientWithExecutor(final ExecutorService executor, final HttpClient httpClient) {
        return (ResteasyClient) new ResteasyClientBuilderImpl()
                .executorService(executor)
                .httpEngine(new JettyClientEngine(httpClient, 0, TimeUnit.MILLISECONDS))
                .build();
    }

    /**
     * {@link ExecutorService} that runs each task on the calling thread.
     */
    private static final class InlineExecutor extends AbstractExecutorService {
        private volatile boolean shutdown;

        @Override
        public void execute(final Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) {
            return true;
        }
    }

    /**
     * Delegating {@link ExecutorService} that opens {@link #completionQueued} when the engine submits its completion
     * task from {@code onHeaders}. The body writer and the tasks of RESTEasy use {@code execute}, and only the
     * completion task uses {@code submit}.
     */
    private static final class SignalingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        final CountDownLatch completionQueued = new CountDownLatch(1);

        SignalingExecutor(final ExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
            completionQueued.countDown();
            return super.newTaskFor(runnable, value);
        }

        @Override
        public void execute(final Runnable command) {
            delegate.execute(command);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }

    /**
     * Delegating {@link ExecutorService} that records whether any {@code submit}/{@code execute}
     * was rejected after the executor had been shut down, so a test can assert it actually hit the
     * rejection path rather than passing via some unrelated fast failure.
     */
    private static final class RejectionRecordingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final AtomicBoolean rejectedAfterShutdown = new AtomicBoolean();

        RejectionRecordingExecutor(final ExecutorService delegate) {
            this.delegate = delegate;
        }

        boolean rejectedAfterShutdown() {
            return rejectedAfterShutdown.get();
        }

        @Override
        public void execute(final Runnable command) {
            try {
                delegate.execute(command);
            } catch (RejectedExecutionException e) {
                if (delegate.isShutdown()) {
                    rejectedAfterShutdown.set(true);
                }
                throw e;
            }
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }

    @Test
    public void testDeferContent() throws Exception {
        server.setHandler(new EchoHandler());
        final byte[] valuableData = randomAlpha().getBytes(StandardCharsets.UTF_8);
        final Response response = client().target(baseUri()).request()
                .post(Entity.entity(new StreamingOutput() {
                    @Override
                    public void write(OutputStream output) throws IOException, WebApplicationException {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                        output.write(valuableData);
                    }
                }, MediaType.APPLICATION_OCTET_STREAM_TYPE));

        assertEquals(200, response.getStatus());
        assertArrayEquals(valuableData, response.readEntity(byte[].class));
    }

    @Test
    public void testFilterBufferReplay() throws Exception {
        final String greeting = "Success";
        final byte[] expected = (greeting).getBytes(StandardCharsets.UTF_8);
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(final Request request, final org.eclipse.jetty.server.Response response,
                    final Callback callback) throws Exception {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                response.setStatus(200);
                response.getHeaders().put(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
                Content.Sink.write(response, true, greeting, callback);
                return true;
            }
        });

        final byte[] content = new byte[expected.length];
        final ClientResponseFilter capturer = new ClientResponseFilter() {
            @Override
            public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) throws IOException {
                responseContext.getEntityStream().read(content);
            }
        };

        try (
                InputStream response = client().register(capturer).target(baseUri()).request()
                        .get(InputStream.class)) {
            // ignored, we are checking filter
        }

        assertArrayEquals(expected, content);
    }

    public URI baseUri() {
        return URI.create("http://localhost:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort());
    }

    /**
     * Reads the whole request body, and only then answers 200.
     */
    static class ReadBodyHandler extends Handler.Abstract {

        @Override
        public boolean handle(final Request request, final org.eclipse.jetty.server.Response response, final Callback callback)
                throws Exception {
            Content.Source.asString(request);
            response.setStatus(200);
            callback.succeeded();
            return true;
        }
    }

    static class EchoHandler extends Handler.Abstract {

        @Override
        public boolean handle(final Request request, final org.eclipse.jetty.server.Response response, final Callback callback)
                throws Exception {
            response.setStatus(200);
            long contentLength = -1;
            for (HttpField field : request.getHeaders()) {
                if (field.getHeader() != null) {
                    switch (field.getHeader()) {
                        case CONTENT_LENGTH -> {
                            response.getHeaders().add(field);
                            contentLength = field.getLongValue();
                        }
                        case CONTENT_TYPE -> response.getHeaders().add(field);
                        case TRAILER -> response.setTrailersSupplier(HttpFields.build());
                        case TRANSFER_ENCODING -> contentLength = Long.MAX_VALUE;
                    }
                }
            }
            if (contentLength > 0)
                Content.copy(request, response, org.eclipse.jetty.server.Response.newTrailersChunkProcessor(response),
                        callback);
            else
                callback.succeeded();
            return true;
        }
    }
}
