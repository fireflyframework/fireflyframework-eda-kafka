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

package org.fireflyframework.eda.testconfig;

import org.fireflyframework.eda.annotation.PublishResult;
import org.fireflyframework.eda.annotation.PublisherType;
import org.fireflyframework.eda.publisher.EventPublisher;
import org.fireflyframework.eda.publisher.EventPublisherFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Test service to demonstrate dynamic topic selection with @PublishResult annotation
 * and programmatic EventPublisher usage.
 */
@Service
public class DynamicTopicTestService {

    @Autowired
    private EventPublisherFactory publisherFactory;

    /**
     * Test @PublishResult with dynamic destination using SpEL
     */
    @PublishResult(
        destination = "#{#result.tenantId}-user-events",
        eventType = "user.registered",
        async = true
    )
    public Mono<TestEventModels.UserRegisteredEvent> createUserWithDynamicTopic(String tenantId, String userId) {
        return Mono.just(TestEventModels.UserRegisteredEvent.builder()
            .userId(userId)
            .tenantId(tenantId)
            .email(userId + "@" + tenantId + ".com")
            .build());
    }

    /**
     * Test @PublishResult with static destination (fallback to default)
     */
    @PublishResult(
        eventType = "user.created",
        async = true
    )
    public Mono<TestEventModels.UserRegisteredEvent> createUserWithDefaultTopic(String userId) {
        return Mono.just(TestEventModels.UserRegisteredEvent.builder()
            .userId(userId)
            .email(userId + "@example.com")
            .build());
    }

    /**
     * Test @PublishResult with conditional destination
     */
    @PublishResult(
        destination = "#{#result.isPremium() ? 'premium-user-events' : 'standard-user-events'}",
        eventType = "user.tier.assigned",
        async = true
    )
    public Mono<TestEventModels.UserRegisteredEvent> createUserWithTierBasedTopic(String userId, boolean isPremium) {
        return Mono.just(TestEventModels.UserRegisteredEvent.builder()
            .userId(userId)
            .email(userId + "@example.com")
            .premium(isPremium)
            .build());
    }

    /**
     * Test programmatic EventPublisher with dynamic destination
     */
    public Mono<Void> publishToCustomTopic(String topicName, TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.APPLICATION_EVENT, topicName);
        
        if (publisher == null) {
            return Mono.error(new IllegalStateException("Publisher not available"));
        }
        
        return publisher.publish(event, null);
    }

    /**
     * Test programmatic EventPublisher with explicit destination override
     */
    public Mono<Void> publishWithExplicitDestination(String customDefault, String explicitDestination, 
                                                     TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.APPLICATION_EVENT, customDefault);
        
        if (publisher == null) {
            return Mono.error(new IllegalStateException("Publisher not available"));
        }
        
        // This should use explicitDestination, not customDefault
        return publisher.publish(event, explicitDestination);
    }

    /**
     * Test multi-platform publishing with dynamic destinations
     */
    public Mono<Void> publishToAllPlatforms(String topicSuffix, TestEventModels.UserRegisteredEvent event) {
        return Mono.when(
            publishToKafkaIfAvailable(topicSuffix, event),
            publishToRabbitMQIfAvailable(topicSuffix, event),
            publishToSpringEvents(topicSuffix, event)
        );
    }

    private Mono<Void> publishToKafkaIfAvailable(String topicSuffix, TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.KAFKA, "kafka-" + topicSuffix);
        
        if (publisher == null) {
            return Mono.empty(); // Kafka not available, skip
        }
        
        return publisher.publish(event, null);
    }

    private Mono<Void> publishToRabbitMQIfAvailable(String topicSuffix, TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.RABBITMQ, "rabbitmq-" + topicSuffix);
        
        if (publisher == null) {
            return Mono.empty(); // RabbitMQ not available, skip
        }
        
        return publisher.publish(event, null);
    }

    private Mono<Void> publishToSpringEvents(String topicSuffix, TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.APPLICATION_EVENT, "spring-" + topicSuffix);
        
        return publisher.publish(event, null);
    }

    /**
     * Test destination resolution priority
     */
    public String testDestinationResolutionPriority(String customDefault, String explicitDestination) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.APPLICATION_EVENT, customDefault);
        
        if (publisher == null) {
            return null;
        }
        
        // Create a test event to see which destination would be used
        TestEventModels.UserRegisteredEvent event = TestEventModels.UserRegisteredEvent.create("test@example.com", "testuser");
        
        // We can't easily test the actual destination used without modifying the publisher,
        // but we can verify the default destination is set correctly
        return publisher.getDefaultDestination();
    }

    /**
     * Test connection-specific dynamic destinations
     */
    public Mono<Void> publishWithSpecificConnection(String connectionId, String destination, 
                                                   TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getPublisherWithDestination(
            PublisherType.APPLICATION_EVENT, connectionId, destination);
        
        if (publisher == null) {
            return Mono.error(new IllegalStateException("Publisher not available for connection: " + connectionId));
        }
        
        return publisher.publish(event, null);
    }

    /**
     * Test default publisher with custom destination
     */
    public Mono<Void> publishWithDefaultPublisher(String customDestination, TestEventModels.UserRegisteredEvent event) {
        EventPublisher publisher = publisherFactory.getDefaultPublisherWithDestination(customDestination);
        
        if (publisher == null) {
            return Mono.error(new IllegalStateException("Default publisher not available"));
        }
        
        return publisher.publish(event, null);
    }
}
