package dev.resteasy.jetty.client.engine;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import jakarta.ws.rs.client.InvocationCallback;

import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.client.Result;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.jboss.resteasy.client.jaxrs.engines.AsyncClientHttpEngine.ResultExtractor;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;

/**
 * Turns the Jetty response events of one exchange into the outcome of {@code future} and {@code callback}.
 * <p>
 * When the status and headers arrived, the call returns that response, and a later failure does not change the
 * outcome. A body that arrived whole stays readable, and a body cut short fails when the caller reads it.
 */
class JettyResponseListener<T> extends InputStreamResponseListener {
    private final ClientInvocation invocation;
    private final boolean buffered;
    private final InvocationCallback<T> callback;
    private final ResultExtractor<T> extractor;
    private final CompletableFuture<T> future;
    private final ExecutorService asyncExecutor;
    private volatile ClientResponse cr;
    private volatile long contentLength = -1;
    private volatile long contentReceived;
    private volatile boolean lastChunkReceived;

    JettyResponseListener(final ClientInvocation invocation, final boolean buffered, final InvocationCallback<T> callback,
            final ResultExtractor<T> extractor, final CompletableFuture<T> future, final ExecutorService asyncExecutor) {
        this.invocation = invocation;
        this.buffered = buffered;
        this.callback = callback;
        this.extractor = extractor;
        this.future = future;
        this.asyncExecutor = asyncExecutor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onHeaders(Response response) {
        super.onHeaders(response);
        contentLength = response.getHeaders().getLongField(HttpHeader.CONTENT_LENGTH);
        InputStream inputStream = getInputStream();
        try {
            final ClientResponse clientResponse = new JettyClientResponse(invocation.getClientConfiguration(), inputStream);
            clientResponse.setProperties(invocation.getMutableProperties());
            clientResponse.setStatus(response.getStatus());
            clientResponse.setHeaders(JettyClientEngine.extract(response.getHeaders()));
            cr = clientResponse;
        } catch (RuntimeException e) {
            // Jetty logs and discards an exception from a listener, which would leave the future incomplete.
            fail(JettyClientEngine.closeSuppressing(inputStream, e));
            return;
        }
        try {
            asyncExecutor.submit(() -> {
                try {
                    if (buffered) {
                        cr.bufferEntity();
                    }
                    complete(extractor == null ? (T) cr : extractor.extractResult(cr));
                } catch (Exception e) {
                    fail(JettyClientEngine.closeSuppressing(inputStream, e));
                }
            });
        } catch (RejectedExecutionException e) {
            // The executor was shut down (e.g. the client was closed) before the completion
            // task could be queued. Jetty considers the exchange successful and would swallow
            // this exception, leaving the future forever incomplete; fail it explicitly instead.
            fail(JettyClientEngine.closeSuppressing(inputStream, e));
        }
    }

    @Override
    public void onContent(Response response, Content.Chunk chunk, Runnable demander) {
        contentReceived += chunk.remaining();
        lastChunkReceived |= chunk.isLast();
        super.onContent(response, chunk, demander);
    }

    @Override
    public void onFailure(Response response, Throwable failure) {
        // Over HTTP/1.1, a request failure also fails a response that the caller did not read to its end, and Jetty
        // fails a 401 without a WWW-Authenticate challenge after it delivered the buffered body as the last chunk. A
        // body whole by its Content-Length, or one that ended with the last chunk, is still the complete message.
        if (wholeBodyReceived()) {
            super.onSuccess(response);
            return;
        }
        super.onFailure(response, failure);
        if (cr == null) {
            JettyClientEngine.failFuture(future, callback, JettyClientEngine.clientException(failure, null));
        }
    }

    @Override
    public void onComplete(Result result) {
        // InputStreamResponseListener discards queued content on any failure, a request failure included.
        if (cr != null && (result.getResponseFailure() == null || wholeBodyReceived())) {
            super.onComplete(new Result(result.getRequest(), result.getResponse()));
        } else {
            super.onComplete(result);
        }
    }

    private boolean wholeBodyReceived() {
        return cr != null && (lastChunkReceived || contentLength >= 0 && contentReceived == contentLength);
    }

    private void complete(T result) {
        if (future.complete(result)) {
            if (callback != null) {
                callback.completed(result);
            }
        } else {
            cr.close();
        }
    }

    private void fail(Throwable failure) {
        JettyClientEngine.failFuture(future, callback, JettyClientEngine.clientException(failure, cr));
    }
}
