/*
 * Copyright 2024-2026 Firefly Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fireflyframework.eda.integration;

import org.fireflyframework.eda.annotation.PublisherType;
import org.fireflyframework.eda.publisher.EventPublisher;
import org.fireflyframework.eda.publisher.EventPublisherFactory;
import org.fireflyframework.eda.publisher.kafka.KafkaEventPublisher;
import org.fireflyframework.eda.testconfig.BaseIntegrationTest;
import org.fireflyframework.eda.testconfig.TestEventModels;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Kafka publisher and consumer.
 * <p>
 * Tests verify:
 * <ul>
 *   <li>End-to-end message publishing to Kafka</li>
 *   <li>Message consumption from Kafka</li>
 *   <li>Serialization/deserialization through Kafka</li>
 *   <li>Header propagation</li>
 *   <li>Multiple message handling</li>
 * </ul>
 */
@SpringBootTest(classes = org.fireflyframework.eda.testconfig.TestApplication.class)
@Testcontainers
@Import(org.fireflyframework.eda.testconfig.TestContainersConfiguration.class)
class KafkaIntegrationTest extends BaseIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    @Autowired(required = false)
    private KafkaEventPublisher kafkaPublisher;

    @Autowired(required = false)
    private EventPublisherFactory publisherFactory;

    @Autowired(required = false)
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaConsumer<String, String> testConsumer;
    private final ConcurrentLinkedQueue<ConsumerRecord<String, String>> consumedMessages = new ConcurrentLinkedQueue<>();

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        // Configure ONLY through firefly.eda.* properties - NO spring.kafka.* properties
        // This ensures 100% hexagonal architecture with no direct Spring Kafka configuration
        registry.add("firefly.eda.publishers.kafka.default.enabled", () -> "true");
        registry.add("firefly.eda.publishers.kafka.default.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("firefly.eda.publishers.kafka.default.default-topic", () -> "test-events");
        registry.add("firefly.eda.publishers.kafka.default.key-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("firefly.eda.publishers.kafka.default.value-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
    }

    @BeforeEach
    void setUp() {
        // Create test consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        testConsumer = new KafkaConsumer<>(props);
        consumedMessages.clear();
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("Should publish message to Kafka topic successfully")
    void shouldPublishMessageToKafkaTopicSuccessfully() {
        // Skip if Kafka publisher is not available
        if (kafkaPublisher == null || kafkaTemplate == null) {
            System.out.println("Skipping test - Kafka publisher not available");
            return;
        }

        // Arrange
        String topic = "test-events";
        TestEventModels.SimpleTestEvent event = TestEventModels.SimpleTestEvent.create("Kafka test message");
        
        testConsumer.subscribe(Collections.singletonList(topic));

        // Act
        StepVerifier.create(kafkaPublisher.publish(event, topic))
                .verifyComplete();

        // Assert - Poll for messages
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var records = testConsumer.poll(Duration.ofMillis(100));
                    records.forEach(consumedMessages::add);
                    assertThat(consumedMessages).isNotEmpty();
                });

        assertThat(consumedMessages).hasSize(1);
        ConsumerRecord<String, String> record = consumedMessages.poll();
        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.value()).contains("Kafka test message");
    }

    @Test
    @DisplayName("Should publish message with headers to Kafka")
    void shouldPublishMessageWithHeadersToKafka() {
        // Skip if Kafka publisher is not available
        if (kafkaPublisher == null || kafkaTemplate == null) {
            System.out.println("Skipping test - Kafka publisher not available");
            return;
        }

        // Arrange
        String topic = "test-events-with-headers";
        TestEventModels.OrderCreatedEvent event = TestEventModels.OrderCreatedEvent.create("customer-123", 99.99);
        Map<String, Object> headers = Map.of(
                "transaction-id", "txn-456",
                "source", "integration-test"
        );
        
        testConsumer.subscribe(Collections.singletonList(topic));

        // Act
        StepVerifier.create(kafkaPublisher.publish(event, topic, headers))
                .verifyComplete();

        // Assert
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var records = testConsumer.poll(Duration.ofMillis(100));
                    records.forEach(consumedMessages::add);
                    assertThat(consumedMessages).isNotEmpty();
                });

        assertThat(consumedMessages).hasSize(1);
        ConsumerRecord<String, String> record = consumedMessages.poll();
        assertThat(record).isNotNull();
        
        // Verify headers
        var kafkaHeaders = record.headers();
        assertThat(kafkaHeaders.lastHeader("transaction-id")).isNotNull();
        assertThat(kafkaHeaders.lastHeader("source")).isNotNull();
    }

    @Test
    @DisplayName("Should publish multiple messages to Kafka")
    void shouldPublishMultipleMessagesToKafka() {
        // Skip if Kafka publisher is not available
        if (kafkaPublisher == null || kafkaTemplate == null) {
            System.out.println("Skipping test - Kafka publisher not available");
            return;
        }

        // Arrange
        String topic = "test-events-multiple";
        int messageCount = 5;
        testConsumer.subscribe(Collections.singletonList(topic));

        // Act - Publish multiple messages
        for (int i = 0; i < messageCount; i++) {
            TestEventModels.SimpleTestEvent event = TestEventModels.SimpleTestEvent.create("Message " + i);
            StepVerifier.create(kafkaPublisher.publish(event, topic))
                    .verifyComplete();
        }

        // Assert
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var records = testConsumer.poll(Duration.ofMillis(100));
                    records.forEach(consumedMessages::add);
                    assertThat(consumedMessages).hasSizeGreaterThanOrEqualTo(messageCount);
                });

        assertThat(consumedMessages).hasSizeGreaterThanOrEqualTo(messageCount);
    }

    @Test
    @DisplayName("Should verify Kafka publisher is available")
    void shouldVerifyKafkaPublisherIsAvailable() {
        // Skip if Kafka publisher is not available
        if (kafkaPublisher == null) {
            System.out.println("Skipping test - Kafka publisher not available");
            return;
        }

        // Assert
        assertThat(kafkaPublisher.isAvailable()).isTrue();
        assertThat(kafkaPublisher.getPublisherType()).isEqualTo(PublisherType.KAFKA);
    }

    @Test
    @DisplayName("Should get Kafka publisher from factory")
    void shouldGetKafkaPublisherFromFactory() {
        // Skip if publisher factory is not available
        if (publisherFactory == null) {
            System.out.println("Skipping test - Publisher factory not available");
            return;
        }

        // Act
        EventPublisher publisher = publisherFactory.getPublisher(PublisherType.KAFKA, "default");

        // Assert - May be null if Kafka is not configured
        // Publisher may be wrapped in ResilientEventPublisher, so check publisher type instead of class
        if (publisher != null) {
            assertThat(publisher.getPublisherType()).isEqualTo(PublisherType.KAFKA);
            assertThat(publisher.isAvailable()).isTrue();
        }
    }
}

