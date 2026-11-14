package io.github.zxkgbwl.llmgateway.playground;

import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 第 2 周第 1 步：Reactor 背压与取消的小实验。
 * 运行参数传 1 / 2 / 2b / 3 / 4，每次只跑一个，方便看日志。
 */
public class ReactorBasics {

    public static void main(String[] args) throws Exception {
        String which = args.length > 0 ? args[0] : "1";
        switch (which) {
            case "1" -> exp1();
            case "2" -> exp2();
            case "2b" -> exp2b();
            case "3" -> exp3();
            case "4" -> exp4();
            case "5" -> exp5();
            case "6" -> exp6();
            case "7" -> exp7();
            case "8" -> exp8();
            default -> System.out.println("参数只能是 1 / 2 / 2b / 3 / 4");
        }
    }

    // 实验 1：完整的信号顺序 onSubscribe → request → onNext ×N → onComplete
    // 注意日志里 request 的数量是多少
    static void exp1() {
        Flux.range(1, 5)
                .log("源")
                .subscribe(i -> System.out.println("收到 " + i));
    }

    // 实验 2：订阅者每次只要 1 个，而且处理得慢
    // 观察：上游是不是收到一个 request(1) 才发一个
    static void exp2() {
        Flux.range(1, 5)
                .log("源")
                .subscribe(new SlowSubscriber<>(200));
    }

    // 实验 2b：按时间发射的源（interval）不会等订阅者，消费跟不上时会怎样
    // 先原样跑一次，再放开 onBackpressureDrop 那行跑一次，对比结果
    static void exp2b() throws InterruptedException {

        SlowSubscriber<Long> subscriber =
                new SlowSubscriber<>(200);

        Flux.interval(Duration.ofMillis(10))
                .onBackpressureDrop(i ->
                        System.out.println("丢弃 " + i))
                .publishOn(Schedulers.boundedElastic(), 4)
                .subscribe(subscriber);

        Thread.sleep(2000);

        System.out.println("主线程准备结束");
        System.out.println("订阅是否已取消：" + subscriber.isDisposed());

        subscriber.dispose();
    }


    // 实验 3：take(5) 拿够之后，cancel 信号如何向上游传播
    // 对比“上游”和“下游”两组日志：谁发出了 cancel，上游什么时候停下来
    static void exp3() {
        Flux.interval(Duration.ofMillis(100))
                .doOnCancel(() -> System.out.println(">>> 上游收到 cancel"))
                .log("上游")
                .take(5)
                .log("下游")
                .blockLast();
    }

    // 实验 4：模拟客户端中途断开，dispose() 就是断开在 Reactor 里的表现
    static void exp4() throws InterruptedException {
        Disposable subscription = Flux.interval(Duration.ofMillis(100))
                .doOnCancel(() -> System.out.println(">>> 上游收到 cancel：网关在这里关闭上游 HTTP 连接"))
                .log("上游")
                .subscribe(i -> System.out.println("客户端收到 " + i));

        Thread.sleep(1000);

        subscription.dispose();

        System.out.println(">>> 客户端断开");
        Thread.sleep(500); // 确认断开之后上游不再发数据
    }

    // 慢订阅者：每次只请求 1 个，处理完一个再请求下一个
    static class SlowSubscriber<T> extends BaseSubscriber<T> {
        private final long delayMillis;

        SlowSubscriber(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(1);
        }

        @Override
        protected void hookOnNext(T value) {
            System.out.println("处理 " + value + "（" + Thread.currentThread().getName() + "）");
            try {
                Thread.sleep(delayMillis); // 模拟处理慢
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            request(1);
        }

        @Override
        protected void hookOnError(Throwable error) {
            System.out.println("出错：" + error);
        }
    }

    static void exp5() {
        // 用计数器模拟“发起了几次上游请求”
        AtomicInteger upstreamCalls = new AtomicInteger();

        Flux<String> stream = Flux.defer(() -> {
                    upstreamCalls.incrementAndGet(); // 每次订阅都会执行到这里，相当于发一次 HTTP 请求
                    return Flux.just("a", "b", "boom", "c");
                })
                .map(s -> {
                    if (s.equals("boom")) throw new IllegalStateException("流中途出错");
                    return s;
                })
                .onErrorResume(e -> Flux.just("StreamError: " + e.getMessage())) // 出错转成一个事件，流正常结束
                .doFinally(signal -> System.out.println(">>> 收尾，结束方式：" + signal));

        stream.subscribe(s -> System.out.println("订阅者 A 收到 " + s));
        stream.subscribe(s -> System.out.println("订阅者 B 收到 " + s));
        System.out.println("上游请求次数：" + upstreamCalls.get()); // 想想这里会打印几
    }

    static void exp6() {
        // 6a：流开始前出错（比如 429），客户端还什么都没收到，可以安全地切换到备用厂商
        Flux<String> primary = Flux.error(new IllegalStateException("429 Too Many Requests"));
        Flux<String> fallback = Flux.just("[备用厂商] ", "你好");
        primary.onErrorResume(e -> {
                    System.out.println(">>> 主厂商失败，切换到备用：" + e.getMessage());
                    return fallback;
                })
                .subscribe(s -> System.out.println("6a 客户端收到 " + s));

        // 6b：流开始后出错，直接重试，看看客户端会收到什么
        AtomicInteger attempts = new AtomicInteger();
        Flux.defer(() -> {
                    int n = attempts.incrementAndGet();
                    // 第一次：发出“你”“好”之后断开；第二次：完整发完
                    Flux<String> tail = (n == 1)
                            ? Flux.error(new IllegalStateException("连接中断"))
                            : Flux.just("！");
                    return Flux.just("你", "好").concatWith(tail);
                })
                .retry(1)
                .subscribe(s -> System.out.println("6b 客户端收到 " + s));
    }

    // 实验 7：两种超时，首 token 超时和空闲超时
    static void exp7() {
        // 场景 A：首 token 迟迟不来（模拟上游排队或者 prefill 太慢）
        Flux<String> slowFirst = Flux.just("a", "b", "c")
                .delaySubscription(Duration.ofMillis(1500));

        // 场景 B：开头正常，中途卡住（模拟上游停止发送数据）
        Flux<String> stalls = Flux.concat(
                Flux.just("a", "b").delayElements(Duration.ofMillis(300)),
                Mono.just("c").delayElement(Duration.ofSeconds(2)));

        withTimeouts("A 首 token 慢", slowFirst);

        withTimeouts("B 中途卡住", stalls);
    }

    // 首 token 超时 1 秒；之后相邻两个元素之间最多间隔 800ms
    static void withTimeouts(String name, Flux<String> source) {
        source.timeout(Mono.delay(Duration.ofSeconds(1)),            // 首 token 超时
                        item -> Mono.delay(Duration.ofMillis(800)))  // 空闲超时：每收到一个元素就重新计时
                .doOnNext(s -> System.out.println(name + " 收到 " + s))
                .onErrorResume(e -> {
                    System.out.println(name + " 超时：" + e.getClass().getSimpleName());
                    return Flux.empty();
                })
                .blockLast();
    }

    // 实验 8：客户端断开时，收尾逻辑是否还会执行
    static void exp8() throws InterruptedException {
        Disposable subscription = Flux.interval(Duration.ofMillis(100))
                .doFinally(signal -> System.out.println(">>> 收尾（结算 usage、记录指标），结束方式：" + signal))
                .subscribe(i -> System.out.println("客户端收到 " + i));
        Thread.sleep(350);
        subscription.dispose(); // 模拟客户端断开
        Thread.sleep(200);
    }


}