package dev.resteasy.jetty.client.engine;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import jakarta.ws.rs.client.InvocationCallback;

import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.Response;
import org.jboss.resteasy.client.jaxrs.engines.AsyncClientHttpEngine.ResultExtractor;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientResponse;

/**
 * Turns the Jetty response events of one exchange into the outcome of {@code future} and {@code callback}.
 */
class JettyResponseListener<T> extends InputStreamResponseListener {
    private final ClientInvocation invocation;
    private final boolean buffered;
    private final InvocationCallback<T> callback;
    private final ResultExtractor<T> extractor;
    private final CompletableFuture<T> future;
    private final ExecutorService asyncExecutor;
    private ClientResponse cr;

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
        InputStream inputStream = getInputStream();
        cr = new JettyClientResponse(invocation.getClientConfiguration(), inputStream);
        cr.setProperties(invocation.getMutableProperties());
        cr.setStatus(response.getStatus());
        cr.setHeaders(JettyClientEngine.extract(response.getHeaders()));
        try {
            asyncExecutor.submit(() -> {
                try {
                    if (buffered) {
                        cr.bufferEntity();
                    }
                    complete(extractor == null ? (T) cr : extractor.extractResult(cr));
                } catch (Exception e) {
                    onFailure(response, JettyClientEngine.closeSuppressing(inputStream, e));
                }
            });
        } catch (RejectedExecutionException e) {
            // The executor was shut down (e.g. the client was closed) before the completion
            // task could be queued. Jetty considers the exchange successful and would swallow
            // this exception, leaving the future forever incomplete; fail it explicitly instead.
            onFailure(response, JettyClientEngine.closeSuppressing(inputStream, e));
        }
    }

    @Override
    public void onFailure(Response response, Throwable failure) {
        super.onFailure(response, failure);
        JettyClientEngine.failFuture(future, callback, JettyClientEngine.clientException(failure, cr));
    }

    private void complete(T result) {
        future.complete(result);
        if (callback != null) {
            callback.completed(result);
        }
    }
}
