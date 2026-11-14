package io.github.zxkgbwl.llmgateway.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 本包内各个 Adapter 共用的小工具。
 *
 * <p>只处理 JSON，不涉及任何 HTTP 细节：怎么发送请求是 StreamingClient 的职责。
 */
final class UpstreamJson {

    // ObjectMapper 是线程安全的，全局共用一个实例即可
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private UpstreamJson() {
    }

    /**
     * 把请求体序列化成 JSON 字符串，用于构造 UpstreamRequest。
     *
     * <p>序列化失败说明请求体本身有问题（属于代码 bug），所以抛非受检异常，不让调用方处理。
     */
    static String toJson(Object body) {
        try {
            return MAPPER.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("无法序列化请求体", e);
        }
    }

    /**
     * 解析流中的一段 JSON。
     *
     * <p>解析失败时抛非受检异常，由 ChatStream 统一转换成 StreamError，Translator 里不需要 try-catch。
     */
    static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("流中出现格式错误的 JSON: " + json, e);
        }
    }

    /** 去掉 baseUrl 末尾的斜杠，避免拼接出 "//v1/messages" 这样的路径。 */
    static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}