package dev.resteasy.jetty.client.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.InvocationCallback;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Result;
import org.eclipse.jetty.client.transport.HttpResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.io.EofException;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocationBuilder;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Calls the Jetty events of one exchange in a fixed order, and runs the queued completion task only when the test
 * says so. Each test is one order of events that a real exchange can produce.
 */
public class JettyResponseListenerTest {
    private final ManualExecutor executor = new ManualExecutor();
    private final CompletableFuture<ClientResponse> future = new CompletableFuture<>();
    private final List<String> outcomes = new CopyOnWriteArrayList<>();
    private ResteasyClient client;
    private ClientInvocation invocation;
    private Request request;
    private HttpResponse response;
    private JettyResponseListener<ClientResponse> listener;

    @BeforeEach
    public void setUp() {
        client = (ResteasyClient) ClientBuilder.newClient();
        invocation = ((ClientInvocationBuilder) client.target("http://localhost/").request()).getClientInvocation();
        request = new HttpClient().newRequest("http://localhost/");
        response = new HttpResponse(request).status(401);
        listener = listener(invocation);
    }

    private JettyResponseListener<ClientResponse> listener(final ClientInvocation clientInvocation) {
        return new JettyResponseListener<>(clientInvocation, false, new InvocationCallback<>() {
            @Override
            public void completed(final ClientResponse result) {
                outcomes.add("completed " + result.getStatus());
            }

            @Override
            public void failed(final Throwable throwable) {
                outcomes.add("failed " + throwable);
            }
        }, null, future, executor);
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    /**
     * Jetty reports a request-side failure, such as a broken pipe on the body write, after the whole response
     * arrived. The body of that response must stay readable.
     */
    @Test
    public void requestFailureAfterWholeResponseKeepsBody() {
        listener.onHeaders(response);
        listener.onContent(response, chunk("nope!"), () -> {
        });
        listener.onSuccess(response);
        listener.onComplete(new Result(request, new EofException("Broken pipe"), response));
        executor.runAll();

        final ClientResponse result = future.getNow(null);
        assertEquals(401, result.getStatus());
        assertEquals("nope!", result.readEntity(String.class));
        assertEquals(List.of("completed 401"), outcomes);
    }

    /**
     * The response fails after its headers and its whole {@code Content-Length} body arrived, before the completion
     * task runs. Jetty does this over HTTP/1.1 when the body write fails before the application read the body, and
     * for a 401 without a challenge. The call returns the response with its body.
     */
    @Test
    public void responseFailureAfterWholeContentLengthBodyKeepsBody() {
        final EofException failure = new EofException("Broken pipe");
        response.headers(headers -> headers.put(HttpHeader.CONTENT_LENGTH, 5));
        listener.onHeaders(response);
        listener.onContent(response, chunk("nope!"), () -> {
        });
        listener.onFailure(response, failure);
        listener.onComplete(new Result(request, failure, response, failure));
        executor.runAll();

        final ClientResponse result = future.getNow(null);
        assertEquals(401, result.getStatus());
        assertEquals("nope!", result.readEntity(String.class));
        assertEquals(List.of("completed 401"), outcomes);
    }

    /**
     * The response fails after its headers arrived but before its whole body arrived, and before the completion
     * task runs. The call returns the response, and the body read reports the failure.
     */
    @Test
    public void responseFailureCutsBodyShort() {
        final EofException failure = new EofException("reset");
        response.headers(headers -> headers.put(HttpHeader.CONTENT_LENGTH, 5));
        listener.onHeaders(response);
        listener.onContent(response, chunk("no"), () -> {
        });
        listener.onFailure(response, failure);
        listener.onComplete(new Result(request, null, response, failure));
        executor.runAll();

        final ClientResponse result = future.getNow(null);
        assertEquals(401, result.getStatus());
        assertThrows(ProcessingException.class, () -> result.readEntity(String.class));
        assertEquals(List.of("completed 401"), outcomes);
    }

    /**
     * The caller cancels the future before the completion task runs. The callback gets no outcome after the
     * cancel, and the engine closes the response that nobody will receive.
     */
    @Test
    public void completionAfterCancelClosesResponseAndSkipsCallback() {
        listener.onHeaders(response);
        assertTrue(future.cancel(false));
        executor.runAll();

        assertEquals(List.of(), outcomes);
        // A closed listener stream aborts the exchange on the next content chunk.
        listener.onContent(response, chunk("late"), () -> {
        });
        assertInstanceOf(AsynchronousCloseException.class, request.getAbortCause());
    }

    /**
     * The caller cancels the future, and then the exchange fails before any response arrived. The callback gets no
     * outcome after the cancel.
     */
    @Test
    public void failureAfterCancelSkipsCallback() {
        assertTrue(future.cancel(false));
        listener.onFailure(response, new EofException("reset"));

        assertEquals(List.of(), outcomes);
    }

    /**
     * Jetty delivers a body that it buffered, such as a 401 without a challenge, as one last chunk, and then fails the
     * response. The body has no {@code Content-Length}, but the last chunk shows that it is whole.
     */
    @Test
    public void responseFailureAfterLastChunkKeepsBody() {
        final EofException failure = new EofException("protocol violation");
        listener.onHeaders(response);
        listener.onContent(response, chunk("nope!", true), () -> {
        });
        listener.onFailure(response, failure);
        listener.onComplete(new Result(request, null, response, failure));
        executor.runAll();

        final ClientResponse result = future.getNow(null);
        assertEquals(401, result.getStatus());
        assertEquals("nope!", result.readEntity(String.class));
        assertEquals(List.of("completed 401"), outcomes);
    }

    /**
     * The engine fails to build the response from the headers. The call fails with that exception, and the callback
     * gets exactly one outcome.
     */
    @Test
    public void responseConstructionFailureFailsCall() {
        final IllegalStateException failure = new IllegalStateException("boom");
        listener = listener(new ClientInvocation(invocation) {
            @Override
            public Map<String, Object> getMutableProperties() {
                throw failure;
            }
        });
        try {
            listener.onHeaders(response);
        } catch (RuntimeException e) {
            // Jetty logs and discards an exception from a listener. The future must not depend on it.
        }
        executor.runAll();

        final CompletionException e = assertThrows(CompletionException.class, () -> future.getNow(null));
        assertEquals(failure, e.getCause().getCause());
        assertEquals(1, outcomes.size());
    }

    private static Content.Chunk chunk(final String content) {
        return chunk(content, false);
    }

    private static Content.Chunk chunk(final String content, final boolean last) {
        return Content.Chunk.from(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)), last);
    }

    /**
     * Holds submitted tasks until {@link #runAll()}, so the test controls when the completion task runs.
     */
    private static final class ManualExecutor extends AbstractExecutorService {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        void runAll() {
            for (Runnable task; (task = tasks.poll()) != null;) {
                task.run();
            }
        }

        @Override
        public void execute(final Runnable command) {
            tasks.add(command);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) {
            return false;
        }
    }
}
