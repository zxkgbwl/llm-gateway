package io.github.zxkgbwl.llmgateway.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 把结算事件发到 Kafka，发完不等结果。
 *
 * <p>KafkaProducer.send 在拿不到元数据或缓冲区满时会同步阻塞（最长 max.block.ms），
 * 所以放到 boundedElastic 上执行，不占 Netty 事件循环。发送失败只记日志，日志文件里的 settlement 行是兜底。
 */
public class KafkaSettlementPublisher implements SettlementListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaSettlementPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String topic;

    public KafkaSettlementPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json, String topic) {
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
    }

    @Override
    public void onSettled(SettlementEvent event) {
        Mono.fromCallable(() -> json.writeValueAsString(event))
                // key = tenantId：同一租户进同一分区，保证有序
                .flatMap(payload -> Mono.fromFuture(() -> kafka.send(topic, event.tenantId(), payload)))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        r -> { },
                        e -> log.warn("结算事件发送 Kafka 失败，以日志为准：requestId={} cause={}",
                                event.requestId(), e.toString()));
    }
}