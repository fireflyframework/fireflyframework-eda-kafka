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

import org.fireflyframework.eda.annotation.ErrorHandlingStrategy;
import org.fireflyframework.eda.annotation.EventListener;
import org.fireflyframework.eda.annotation.PublisherType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test event listeners for integration testing.
 * These listeners capture events for verification in tests.
 */
@Component
public class TestEventListeners {

    private static final Logger log = LoggerFactory.getLogger(TestEventListeners.class);

    private final List<TestEventModels.SimpleTestEvent> simpleEvents = new CopyOnWriteArrayList<>();
    private final List<TestEventModels.OrderCreatedEvent> orderEvents = new CopyOnWriteArrayList<>();
    private final List<TestEventModels.UserRegisteredEvent> userEvents = new CopyOnWriteArrayList<>();
    private final List<Map<String, Object>> receivedHeaders = new CopyOnWriteArrayList<>();

    @EventListener(
            destinations = {"test-exchange/test.routing.key", "test-consumer-exchange-*/test.routing.key", "test-consumer-topic-.*"},
            consumerType = PublisherType.AUTO,
            errorStrategy = ErrorHandlingStrategy.LOG_AND_CONTINUE
    )
    public void handleSimpleEvent(TestEventModels.SimpleTestEvent event, Map<String, Object> headers) {
        log.info("🎯 [Test Listener] Received SimpleTestEvent: {}", event.getMessage());
        log.debug("Headers: {}", headers);
        simpleEvents.add(event);
        receivedHeaders.add(headers);
    }

    @EventListener(
            destinations = {"test-exchange-headers/test.headers", "test-consumer-exchange-*/test.headers"},
            consumerType = PublisherType.AUTO,
            errorStrategy = ErrorHandlingStrategy.LOG_AND_CONTINUE
    )
    public void handleOrderEvent(TestEventModels.OrderCreatedEvent event, Map<String, Object> headers) {
        log.info("🎯 [Test Listener] Received OrderCreatedEvent: customerId={}, amount={}",
                event.getCustomerId(), event.getAmount());
        log.debug("Headers: {}", headers);
        orderEvents.add(event);
        receivedHeaders.add(headers);
    }

    @EventListener(
            destinations = {"*"},
            consumerType = PublisherType.AUTO,
            errorStrategy = ErrorHandlingStrategy.LOG_AND_CONTINUE
    )
    public void handleUserEvent(TestEventModels.UserRegisteredEvent event, Map<String, Object> headers) {
        log.info("🎯 [Test Listener] Received UserRegisteredEvent: userId={}, email={}",
                event.getUserId(), event.getEmail());
        log.debug("Headers: {}", headers);
        userEvents.add(event);
        receivedHeaders.add(headers);
    }

    public List<TestEventModels.SimpleTestEvent> getSimpleEvents() {
        return simpleEvents;
    }

    public List<TestEventModels.OrderCreatedEvent> getOrderEvents() {
        return orderEvents;
    }

    public List<TestEventModels.UserRegisteredEvent> getUserEvents() {
        return userEvents;
    }

    public List<Map<String, Object>> getReceivedHeaders() {
        return receivedHeaders;
    }

    public void clear() {
        log.info("🧹 Clearing all captured events");
        simpleEvents.clear();
        orderEvents.clear();
        userEvents.clear();
        receivedHeaders.clear();
    }

    public int getTotalEventsReceived() {
        return simpleEvents.size() + orderEvents.size() + userEvents.size();
    }
}

