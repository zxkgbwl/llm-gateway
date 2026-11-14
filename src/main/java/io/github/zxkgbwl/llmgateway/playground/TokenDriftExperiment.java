package io.github.zxkgbwl.llmgateway.playground;



// import 按实际包路径调整（TokenEstimator）

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zxkgbwl.llmgateway.token.TokenEstimator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 第 5 步：本地估算值与 Gemini 官方计数的偏差。
 * 用 countTokens 接口计数，只计数不生成，不消耗生成额度。
 */
public class TokenDriftExperiment {

    private static final String MODEL = "gemini-3.1-flash-lite";
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 三类样本；最好换成约 1000 字符的真实文本，比值会更稳定
        Map<String, String> samples = new LinkedHashMap<>();
        samples.put("中文", "大模型网关位于客户端与模型厂商之间，负责统一协议、限流计费、故障切换和可观测性。"
                + "由于各家厂商使用不同的分词器，同一段文本在不同模型下的 token 数并不相同，"
                + "网关在请求发出前只能基于本地估算进行预扣，请求结束后再按厂商返回的实际用量结算。");
        samples.put("英文", "An LLM gateway sits between clients and model providers. It unifies protocols, "
                + "enforces rate limits, handles failover, and exposes metrics. Because every provider uses "
                + "its own tokenizer, the gateway can only pre-authorize an estimate before the request "
                + "and must settle against the usage the provider reports afterwards.");
        samples.put("代码", """
                public List<StreamEvent> translate(SseEvent sse) {
                    JsonNode node = AdapterSupport.parse(sse.data());
                    String type = node.path("type").asText();
                    return switch (type) {
                        case "message_stop" -> List.of(new Finished(mapStopReason(raw), raw));
                        default -> List.of();
                    };
                }
                """);

        TokenEstimator.warmUp();
        HttpClient http = HttpClient.newHttpClient();
        String key = System.getenv("GEMINI_API_KEY");

        System.out.printf("%-4s %8s %8s %8s%n", "类型", "本地估算", "Gemini", "比值");
        for (Map.Entry<String, String> e : samples.entrySet()) {
            int local = TokenEstimator.o200k(e.getValue()); // 方法名按你的代码调整
            int actual = countTokens(http, key, e.getValue());
            // 比值 = 实际 / 估算；大于 1 说明本地估少了
            System.out.printf("%-4s %8d %8d %8.2f%n", e.getKey(), local, actual, (double) actual / local);
        }
    }

    // 调用 Gemini countTokens，返回 totalTokens
    private static int countTokens(HttpClient http, String key, String text) throws Exception {
        String body = JSON.writeValueAsString(
                Map.of("contents", List.of(Map.of("parts", List.of(Map.of("text", text))))));
        HttpRequest request = HttpRequest.newBuilder(URI.create(
                        "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL + ":countTokens"))
                .header("x-goog-api-key", key)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("countTokens 失败：" + response.statusCode() + " " + response.body());
        }
        return JSON.readTree(response.body()).path("totalTokens").asInt();
    }
}