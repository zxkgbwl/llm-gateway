package io.github.zxkgbwl.llmgateway.token;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;

/**
 * Pre-request token estimation, used later for quota reservation.
 *
 * <p>jtokkit implements OpenAI's tokenizers only, so estimates for Claude and Gemini are
 * approximate. Measuring how far off they are is part of the week-1 experiments.
 */
public final class TokenEstimator {

    // The lazy registry loads an encoding's vocabulary only when it is first requested,
    // instead of loading every encoding up front.
    private static final EncodingRegistry REGISTRY = Encodings.newLazyEncodingRegistry();

    private TokenEstimator() {
    }

    /**
     * Loads the vocabularies now. Call once at startup so that the first real request
     * does not pay the loading cost.
     */
    public static void warmUp() {
        o200k("");
        cl100k("");
    }

    public static int o200k(String text) {
        return REGISTRY.getEncoding(EncodingType.O200K_BASE).countTokens(text);
    }

    public static int cl100k(String text) {
        return REGISTRY.getEncoding(EncodingType.CL100K_BASE).countTokens(text);
    }
}
