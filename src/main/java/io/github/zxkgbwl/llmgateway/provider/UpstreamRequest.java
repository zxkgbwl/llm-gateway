package io.github.zxkgbwl.llmgateway.provider;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A transport-neutral description of one upstream call, produced by a ProtocolAdapter.
 *
 * <p>Always a POST with a JSON body that expects an SSE response; the transport adds
 * Content-Type, Accept and timeouts. Adapters only supply what differs per provider:
 * the URL, provider-specific headers (auth, API version) and the serialized body.
 */
public record UpstreamRequest(URI uri, Map<String, String> headers, String jsonBody) {

    private static final Set<String> SECRET_HEADERS = Set.of("authorization", "x-api-key", "x-goog-api-key");

    public UpstreamRequest {
        if (uri == null || !uri.isAbsolute()) {
            throw new IllegalArgumentException("uri must be absolute: " + uri);
        }
        if (jsonBody == null || jsonBody.isBlank()) {
            throw new IllegalArgumentException("jsonBody must not be empty");
        }
        headers = Map.copyOf(headers);
    }

    /** Never prints credentials: requests end up in logs and exception messages. */
    @Override
    public String toString() {
        String safeHeaders = headers.entrySet().stream()
                .map(e -> e.getKey() + "=" + (isSecret(e.getKey()) ? "***" : e.getValue()))
                .collect(Collectors.joining(", ", "{", "}"));
        return "UpstreamRequest[uri=" + uri + ", headers=" + safeHeaders + ", jsonBody=" + jsonBody + "]";
    }

    private static boolean isSecret(String headerName) {
        return SECRET_HEADERS.contains(headerName.toLowerCase(Locale.ROOT));
    }
}
