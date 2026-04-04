package com.loomq.wal.v2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RingBuffer 单元测试
 */
class RingBufferTest {

    private RingBuffer<String> buffer;

    @BeforeEach
    void setUp() {
        buffer = new RingBuffer<>(16);  // 小容量方便测试
    }

    @Test
    @DisplayName("容量必须是2的幂次方")
    void capacityMustBePowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer<>(10));
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer<>(0));
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer<>(-1));

        // 有效的容量
        new RingBuffer<>(1);
        new RingBuffer<>(2);
        new RingBuffer<>(4);
        new RingBuffer<>(8);
        new RingBuffer<>(1024);
        new RingBuffer<>(16384);
    }

    @Test
    @DisplayName("基本写入和读取")
    void basicOfferAndPoll() {
        assertTrue(buffer.offer("item1"));
        assertTrue(buffer.offer("item2"));

        assertEquals(2, buffer.size());

        assertEquals("item1", buffer.poll());
        assertEquals("item2", buffer.poll());
        assertNull(buffer.poll());
    }

    @Test
    @DisplayName("空缓冲区应返回null")
    void emptyBufferReturnsNull() {
        assertNull(buffer.poll());
        assertTrue(buffer.isEmpty());
        assertEquals(0, buffer.size());
    }

    @Test
    @DisplayName("满缓冲区应拒绝写入")
    void fullBufferRejectsOffer() {
        // 填满缓冲区
        for (int i = 0; i < 16; i++) {
            assertTrue(buffer.offer("item-" + i), "Should accept item " + i);
        }

        assertTrue(buffer.isFull());
        assertEquals(16, buffer.size());

        // 再写入应该失败
        assertFalse(buffer.offer("overflow"));

        // 读取一个后可以写入
        buffer.poll();
        assertTrue(buffer.offer("new-item"));
    }

    @Test
    @DisplayName("批量消费应高效")
    void batchDrain() {
        // 写入10个元素
        for (int i = 0; i < 10; i++) {
            buffer.offer("item-" + i);
        }

        List<String> consumed = new ArrayList<>();
        int count = buffer.drain(consumed::add, 5);

        assertEquals(5, count);
        assertEquals(5, consumed.size());
        assertEquals("item-0", consumed.get(0));
        assertEquals("item-4", consumed.get(4));
        assertEquals(5, buffer.size());

        // 消费剩余
        count = buffer.drain(consumed::add, 10);
        assertEquals(5, count);
        assertEquals(10, consumed.size());
    }

    @Test
    @DisplayName("drainToList应收集所有元素")
    void drainToList() {
        for (int i = 0; i < 5; i++) {
            buffer.offer("item-" + i);
        }

        List<String> result = buffer.drainToList(10);

        assertEquals(5, result.size());
        assertEquals(0, buffer.size());
    }

    @Test
    @DisplayName("null元素应抛出异常")
    void nullItemNotAllowed() {
        assertThrows(NullPointerException.class, () -> buffer.offer(null));
    }

    @Test
    @DisplayName("清除应清空所有元素")
    void clearBuffer() {
        for (int i = 0; i < 10; i++) {
            buffer.offer("item-" + i);
        }

        assertEquals(10, buffer.size());

        buffer.clear();

        assertEquals(0, buffer.size());
        assertTrue(buffer.isEmpty());
        assertNull(buffer.poll());
    }

    @Test
    @DisplayName("统计信息应正确")
    void statsTracking() {
        var stats = buffer.getStats();

        assertEquals(0, stats.getWriteCount());
        assertEquals(0, stats.getReadCount());
        assertEquals(0, stats.getOverflowCount());

        buffer.offer("item1");
        buffer.offer("item2");
        buffer.poll();

        assertEquals(2, stats.getWriteCount());
        assertEquals(1, stats.getReadCount());

        // 填满并触发溢出
        buffer.clear();
        for (int i = 0; i < 16; i++) {
            buffer.offer("item");
        }
        buffer.offer("overflow");  // 应该失败并记录溢出

        assertEquals(1, stats.getOverflowCount());
    }

    @Test
    @DisplayName("单生产者单消费者应正确")
    void singleProducerSingleConsumer() throws InterruptedException {
        RingBuffer<Integer> queue = new RingBuffer<>(1024);
        int itemCount = 10000;

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch latch = new CountDownLatch(1);

        // 生产者
        executor.submit(() -> {
            for (int i = 0; i < itemCount; i++) {
                while (!queue.offer(i)) {
                    Thread.yield();  // 缓冲区满时自旋
                }
            }
            latch.countDown();
        });

        // 消费者
        List<Integer> consumed = new ArrayList<>();
        while (consumed.size() < itemCount) {
            Integer item = queue.poll();
            if (item != null) {
                consumed.add(item);
            }
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(itemCount, consumed.size());
        for (int i = 0; i < itemCount; i++) {
            assertEquals(i, consumed.get(i));
        }
    }

    @Test
    @DisplayName("多生产者单消费者应正确")
    void multiProducerSingleConsumer() throws InterruptedException {
        RingBuffer<Integer> queue = new RingBuffer<>(4096);
        int producerCount = 4;
        int itemsPerProducer = 10000;
        int totalItems = producerCount * itemsPerProducer;

        ExecutorService executors = Executors.newFixedThreadPool(producerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(producerCount);

        // 启动生产者
        for (int p = 0; p < producerCount; p++) {
            final int producerId = p;
            executors.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < itemsPerProducer; i++) {
                        int value = producerId * itemsPerProducer + i;
                        while (!queue.offer(value)) {
                            Thread.yield();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        // 开始生产
        startLatch.countDown();

        // 消费
        AtomicInteger consumedCount = new AtomicInteger(0);
        while (consumedCount.get() < totalItems) {
            int count = queue.drain(item -> consumedCount.incrementAndGet(), 100);
            if (count == 0) {
                Thread.yield();
            }
        }

        assertTrue(completeLatch.await(30, TimeUnit.SECONDS));
        executors.shutdown();

        assertEquals(totalItems, consumedCount.get());
    }

    @Test
    @DisplayName("环绕测试 - 序列号回绕")
    void sequenceWrapAround() {
        // 使用小容量测试环绕
        RingBuffer<Integer> smallBuffer = new RingBuffer<>(4);

        // 填满 -> 读取 -> 再填满（触发环绕）
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < 4; i++) {
                assertTrue(smallBuffer.offer(round * 4 + i));
            }
            assertTrue(smallBuffer.isFull());

            for (int i = 0; i < 4; i++) {
                assertEquals(round * 4 + i, smallBuffer.poll());
            }
            assertTrue(smallBuffer.isEmpty());
        }
    }

    @Test
    @DisplayName("性能测试 - 单线程写入吞吐")
    void singleThreadWriteThroughput() {
        RingBuffer<Integer> queue = new RingBuffer<>(RingBuffer.DEFAULT_CAPACITY);
        int iterations = 10_000_000;

        long start = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            while (!queue.offer(i)) {
                // 自旋等待
            }
            // 立即消费避免满
            if (i % 1000 == 0) {
                queue.drain(item -> {}, 1000);
            }
        }

        long elapsed = System.nanoTime() - start;
        double opsPerSecond = iterations / (elapsed / 1_000_000_000.0);

        System.out.println("Single-thread write throughput: " + String.format("%.2f", opsPerSecond) + " ops/s");
        assertTrue(opsPerSecond > 1_000_000, "Should achieve > 1M ops/s");
    }
}
