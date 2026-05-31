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

package org.fireflyframework.eda.publisher.kafka;

import org.fireflyframework.eda.annotation.PublisherType;
import org.fireflyframework.eda.properties.EdaProperties;
import org.fireflyframework.eda.publisher.ConnectionAwarePublisher;
import org.fireflyframework.eda.publisher.EventPublisher;
import org.fireflyframework.eda.publisher.PublisherHealth;
import org.fireflyframework.eda.serialization.MessageSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka event publisher implementation using Spring Kafka.
 * <p>
 * This publisher provides:
 * <ul>
 *   <li>Async publishing using KafkaTemplate and reactor</li>
 *   <li>Proper header and partitioning support</li>
 *   <li>Connection health monitoring</li>
 *   <li>Comprehensive error handling</li>
 * </ul>
 */
@Component
@ConditionalOnClass(KafkaTemplate.class)
@Slf4j
public class KafkaEventPublisher implements EventPublisher, ConnectionAwarePublisher {

    private final ObjectProvider<KafkaTemplate<String, Object>> kafkaTemplateProvider;
    private final MessageSerializer messageSerializer;
    private final EdaProperties edaProperties;
    private String connectionId = "default";

    public KafkaEventPublisher(
            @org.springframework.beans.factory.annotation.Qualifier("fireflyEdaKafkaTemplate")
            ObjectProvider<KafkaTemplate<String, Object>> kafkaTemplateProvider,
            MessageSerializer messageSerializer,
            EdaProperties edaProperties) {
        this.kafkaTemplateProvider = kafkaTemplateProvider;
        this.messageSerializer = messageSerializer;
        this.edaProperties = edaProperties;
    }

    @Override
    public Mono<Void> publish(Object event, String destination, Map<String, Object> headers) {
        return Mono.fromCallable(() -> {
            try {
                // Use default destination if none provided
                String effectiveDestination = destination != null ? destination : getDefaultDestination();
                log.debug("Publishing event to Kafka topic: {}", effectiveDestination);

                KafkaTemplate<String, Object> kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
                if (kafkaTemplate == null) {
                    throw new IllegalStateException("KafkaTemplate is not available");
                }

                // Serialize the event
                byte[] serializedEventBytes = messageSerializer.serialize(event);
                String serializedEvent = new String(serializedEventBytes, java.nio.charset.StandardCharsets.UTF_8);

                // Create producer record
                String key = extractPartitionKey(headers, event);
                ProducerRecord<String, Object> record = new ProducerRecord<>(effectiveDestination, key, serializedEvent);

                // Add headers to Kafka record
                addKafkaHeaders(record, headers, event);

                // Send asynchronously
                CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);
                
                return future.thenApply(result -> {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.debug("Successfully published event to Kafka: topic={}, partition={}, offset={}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                    return null; // Convert to Void
                });

            } catch (Exception e) {
                log.error("Failed to publish event to Kafka topic: {}", destination, e);
                CompletableFuture<Void> failedFuture = new CompletableFuture<>();
                failedFuture.completeExceptionally(new RuntimeException("Failed to publish event to Kafka", e));
                return failedFuture;
            }
        })
        .flatMap(future -> Mono.fromFuture(future))
        .then()
        .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public PublisherType getPublisherType() {
        return PublisherType.KAFKA;
    }

    @Override
    public String getConnectionId() {
        return connectionId;
    }

    @Override
    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId != null ? connectionId : "default";
    }

    @Override
    public boolean isConnectionConfigured(String connectionId) {
        return kafkaTemplateProvider.getIfAvailable() != null;
    }

    @Override
    public Mono<PublisherHealth> getHealth() {
        return Mono.fromCallable(() -> {
            PublisherHealth.PublisherHealthBuilder healthBuilder = PublisherHealth.builder()
                    .publisherType(getPublisherType())
                    .connectionId(getConnectionId())
                    .lastChecked(Instant.now());

            try {
                KafkaTemplate<String, Object> kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
                
                if (kafkaTemplate != null) {
                    return healthBuilder
                            .status("UP")
                            .available(true)
                            .details(Map.of(
                                    "kafka_template", kafkaTemplate.getClass().getSimpleName(),
                                    "connection_id", getConnectionId()
                            ))
                            .build();
                } else {
                    return healthBuilder
                            .status("DOWN")
                            .available(false)
                            .errorMessage("KafkaTemplate is not available")
                            .build();
                }

            } catch (Exception e) {
                log.warn("Kafka health check failed for connection: {}", getConnectionId(), e);
                return healthBuilder
                        .status("DOWN")
                        .available(false)
                        .errorMessage("Kafka health check error: " + e.getMessage())
                        .details(Map.of("error_type", e.getClass().getSimpleName()))
                        .build();
            }
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public boolean isAvailable() {
        return kafkaTemplateProvider.getIfAvailable() != null;
    }

    @Override
    public String getDefaultDestination() {
        EdaProperties.Publishers.KafkaConfig config =
            (EdaProperties.Publishers.KafkaConfig) edaProperties.getPublisherConfig(PublisherType.KAFKA, connectionId);
        return config != null ? config.getDefaultTopic() : "events";
    }

    /**
     * Extracts partition key from headers or generates one from event.
     */
    private String extractPartitionKey(Map<String, Object> headers, Object event) {
        if (headers != null) {
            Object partitionKey = headers.get("partition_key");
            if (partitionKey != null) {
                return partitionKey.toString();
            }
            
            Object transactionId = headers.get("transaction_id");
            if (transactionId != null) {
                return transactionId.toString();
            }
        }
        
        // Use event class name as fallback
        return event != null ? event.getClass().getSimpleName() : "unknown";
    }

    /**
     * Adds headers to Kafka producer record.
     */
    private void addKafkaHeaders(ProducerRecord<String, Object> record, Map<String, Object> headers, Object event) {
        // Add custom headers
        if (headers != null) {
            headers.forEach((key, value) -> {
                if (value != null) {
                    record.headers().add(key, value.toString().getBytes());
                }
            });
        }

        // Add system headers
        record.headers().add("publisher_type", getPublisherType().name().getBytes());
        record.headers().add("connection_id", getConnectionId().getBytes());
        record.headers().add("event_class", event.getClass().getName().getBytes());
        record.headers().add("published_at", Instant.now().toString().getBytes());
    }
}