# Firefly Framework - EDA Kafka

[![CI](https://github.com/fireflyframework/fireflyframework-eda-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-eda-kafka/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> Apache Kafka transport adapter for the Firefly Framework event-driven architecture (EDA) abstraction — reactive event publishing and dynamic, annotation-driven consumption over Kafka, configured entirely through `firefly.eda.*`.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [How It Works](#how-it-works)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

`fireflyframework-eda-kafka` is the **Apache Kafka transport adapter** for the Firefly EDA abstraction. It plugs into the EDA core (`fireflyframework-eda`) and implements its publisher and consumer ports — `EventPublisher` and `EventConsumer` — using Apache Kafka as the underlying broker. Applications interact only with the framework's transport-agnostic API (`@EventPublisher`, `@PublishResult`, `@EventListener`); this module turns those calls into Kafka produce/consume operations.

Kafka is one of several interchangeable transports in the EDA family. The active transport is selected per publish/listen via the `PublisherType` (selection order `KAFKA → RABBITMQ → POSTGRES → APPLICATION_EVENT`, or `AUTO` to resolve from configuration). To route events over Kafka you add this adapter to the classpath and enable it through `firefly.eda.publishers.kafka.*` / `firefly.eda.consumer.kafka.*`. Sibling transport adapters cover other brokers:

- **`fireflyframework-eda-kafka`** (this module) — Apache Kafka, via Spring Kafka.
- **`fireflyframework-eda-rabbitmq`** — RabbitMQ.
- **`fireflyframework-eda-postgres`** — PostgreSQL `LISTEN/NOTIFY` with a transactional outbox.

The adapter is **100% hexagonal**: it is driven exclusively by `firefly.eda.*` properties and intentionally ignores Spring Kafka's native `spring.kafka.*` configuration, keeping a single, consistent configuration surface across every Firefly EDA transport. The producer side (`ProducerFactory` / `KafkaTemplate`) is wired by `FireflyEdaKafkaPublisherAutoConfiguration`, the consumer side (`ConsumerFactory` / `ConcurrentKafkaListenerContainerFactory`) by `FireflyEdaKafkaConsumerAutoConfiguration`, and the `KafkaEventPublisher` / `KafkaEventConsumer` beans are discovered by the EDA core component scan. A `FireflyEdaKafkaEnvironmentPostProcessor` conditionally excludes Spring Boot's stock `KafkaAutoConfiguration` when the Kafka transport is not enabled, preventing spurious connection attempts and health checks at startup.

## Features

- **Reactive `EventPublisher`** (`KafkaEventPublisher`) — non-blocking publishing on `Schedulers.boundedElastic()`, wrapping `KafkaTemplate.send(...)` in a `Mono<Void>`.
- **Dynamic, annotation-driven `EventConsumer`** (`KafkaEventConsumer`) — subscribes via a `@KafkaListener` whose `topicPattern` is built at runtime from discovered `@EventListener(consumerType = KAFKA | AUTO)` annotations, with hot topic refresh when listeners change.
- **Firefly-owned infrastructure beans** — `fireflyEdaKafkaProducerFactory`, `fireflyEdaKafkaTemplate`, `fireflyEdaKafkaConsumerFactory`, and `fireflyEdaKafkaListenerContainerFactory`, all created from `firefly.eda.*` and all `@ConditionalOnMissingBean` so you can override them.
- **100% hexagonal configuration** — driven by `firefly.eda.publishers.kafka.*` and `firefly.eda.consumer.kafka.*`; `spring.kafka.*` is ignored.
- **Header propagation and partitioning** — copies your event headers onto the Kafka record, adds system headers (`publisher_type`, `connection_id`, `event_class`, `published_at`), and derives the partition key from `partition_key` / `transaction_id` headers (falling back to the event class name).
- **Type-aware deserialization on consume** — uses the `event_class` header to reconstruct the original event type, with a graceful fallback to generic `Object` deserialization.
- **Connection health monitoring** — both publisher and consumer expose `PublisherHealth` / `ConsumerHealth` (status, availability, group ID, live dynamic topic set) for actuator-style introspection.
- **Conditional auto-config exclusion** — `KafkaAutoConfiguration` is excluded automatically when neither Kafka publisher nor consumer is enabled and no `spring.kafka.bootstrap-servers` is set.
- **Spring Boot auto-configuration** — registered via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`; the environment post-processor via `META-INF/spring.factories`.

## Requirements

- Java 21+ (Java 25 recommended)
- Spring Boot 3.x
- Maven 3.9+
- An Apache Kafka broker (reachable at the configured `bootstrap-servers`)
- `fireflyframework-eda` (the EDA core; pulled in transitively)

## Installation

Add the dependency. The version is managed by the Firefly BOM / parent, so you normally omit `<version>`:

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-eda-kafka</artifactId>
    <!-- version managed by the Firefly BOM / fireflyframework-parent -->
</dependency>
```

This adapter declares `fireflyframework-eda` (the EDA core) as a dependency, so adding it brings in the full EDA programming model transitively. If you manage versions explicitly outside the BOM, align the version with the rest of your Firefly modules.

## Quick Start

**1. Add the dependency** (see above). The Kafka transport beans light up only when you enable them.

**2. Enable and configure the Kafka transport** in `application.yml`:

```yaml
firefly:
  eda:
    publishers:
      enabled: true
      kafka:
        default:
          enabled: true
          bootstrap-servers: localhost:9092
          default-topic: events
    consumer:
      enabled: true
      group-id: my-service
      kafka:
        default:
          enabled: true
          bootstrap-servers: localhost:9092
          topics: events
          auto-offset-reset: earliest
```

**3. Publish events** with the framework's transport-agnostic annotation — the result of the method is published to Kafka:

```java
@Service
public class PaymentService {

    @PublishResult(publisherType = PublisherType.KAFKA,
                   destination = "payments",
                   eventType = "payment.completed")
    public Mono<PaymentCompleted> complete(PaymentCommand cmd) {
        return process(cmd); // returned value is published to the "payments" topic
    }
}
```

**4. Consume events** by annotating a handler — the Kafka consumer discovers the topic automatically and subscribes:

```java
@Component
public class PaymentListener {

    @EventListener(destinations = "payments",
                   eventTypes = "payment.completed",
                   consumerType = PublisherType.KAFKA)
    public Mono<Void> onPaymentCompleted(PaymentCompleted event) {
        log.info("Received payment: {}", event.id());
        return Mono.empty();
    }
}
```

No `@KafkaListener` of your own and no `spring.kafka.*` is required — the adapter owns the `KafkaTemplate`, the listener container factory, and the dynamic topic pattern.

## Configuration

All settings live under `firefly.eda.*`. The Kafka publisher and consumer are **opt-in** (`enabled: false` by default at every level), so nothing connects to a broker until you turn it on. Values below are the real defaults from `EdaProperties`.

```yaml
firefly:
  eda:
    enabled: true                       # master switch for EDA (default: true)
    publishers:
      enabled: false                    # enable the publisher side (default: false)
      kafka:
        default:                        # connection id; add more named connections as needed
          enabled: false                # enable the Kafka publisher (default: false)
          bootstrap-servers:            # e.g. localhost:9092 (required to create beans)
          default-topic: events         # topic used when none is specified (default: events)
          key-serializer: org.apache.kafka.common.serialization.StringSerializer
          value-serializer: org.apache.kafka.common.serialization.StringSerializer
          properties: {}                # extra raw Kafka producer properties
    consumer:
      enabled: false                    # enable the consumer side (default: false)
      group-id: firefly-eda             # Kafka consumer group id (default: firefly-eda)
      kafka:
        default:
          enabled: false                # enable the Kafka consumer (default: false)
          bootstrap-servers:            # e.g. localhost:9092 (required to create beans)
          topics: events                # fallback topic pattern when no @EventListener topics are found
          auto-offset-reset: earliest   # default: earliest
          key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          properties: {}                # extra raw Kafka consumer properties
```

Key properties:

| Property | Default | Purpose |
| --- | --- | --- |
| `firefly.eda.publishers.enabled` | `false` | Global publisher switch; must be `true` to create Kafka producer beans. |
| `firefly.eda.publishers.kafka.default.enabled` | `false` | Enables the Kafka publisher for the `default` connection. |
| `firefly.eda.publishers.kafka.default.bootstrap-servers` | _(none)_ | Broker list; **beans are only created when this is non-empty**. |
| `firefly.eda.publishers.kafka.default.default-topic` | `events` | Destination used when a publish call supplies no topic. |
| `firefly.eda.publishers.kafka.default.key-serializer` / `value-serializer` | `StringSerializer` | Producer serializers. |
| `firefly.eda.publishers.kafka.default.properties` | `{}` | Pass-through map of raw Kafka producer config. |
| `firefly.eda.consumer.enabled` | `false` | Global consumer switch; required for the consumer beans and `KafkaEventConsumer`. |
| `firefly.eda.consumer.group-id` | `firefly-eda` | Consumer group id applied to the listener. |
| `firefly.eda.consumer.kafka.default.enabled` | `false` | Enables the Kafka consumer for the `default` connection. |
| `firefly.eda.consumer.kafka.default.bootstrap-servers` | _(none)_ | Broker list; **beans are only created when this is non-empty**. |
| `firefly.eda.consumer.kafka.default.topics` | `events` | Fallback topic pattern when no `@EventListener` Kafka topics are discovered. |
| `firefly.eda.consumer.kafka.default.auto-offset-reset` | `earliest` | Offset reset policy. |
| `firefly.eda.consumer.kafka.default.key-deserializer` / `value-deserializer` | `StringDeserializer` | Consumer deserializers. |
| `firefly.eda.consumer.kafka.default.properties` | `{}` | Pass-through map of raw Kafka consumer config. |

> **Note:** `spring.kafka.*` properties are intentionally **not** consulted by this adapter. If `spring.kafka.bootstrap-servers` is set explicitly, the adapter steps aside and leaves Spring Boot's own `KafkaAutoConfiguration` in place. Multiple named connections are supported by adding keys alongside `default` (e.g. `firefly.eda.publishers.kafka.audit.*`).

## How It Works

1. **Startup gating** — `FireflyEdaKafkaEnvironmentPostProcessor` runs first and excludes Spring Boot's `KafkaAutoConfiguration` unless the Kafka publisher or consumer is enabled (or `spring.kafka.bootstrap-servers` is explicitly set).
2. **Publisher wiring** — `FireflyEdaKafkaPublisherAutoConfiguration` creates `fireflyEdaKafkaProducerFactory` and `fireflyEdaKafkaTemplate` only when publishers are enabled, the Kafka publisher is enabled, and `bootstrap-servers` is non-empty. `KafkaEventPublisher` consumes the template via an `ObjectProvider`.
3. **Consumer wiring** — `FireflyEdaKafkaConsumerAutoConfiguration` creates `fireflyEdaKafkaConsumerFactory` and the `@Primary` `fireflyEdaKafkaListenerContainerFactory` under the equivalent conditions. `KafkaEventConsumer` registers a single `@KafkaListener` whose `topicPattern` is computed from discovered `@EventListener` annotations.
4. **Dynamic topics** — when listeners are added at runtime, `EventListenerProcessor` notifies the consumer, which restarts its listener containers to pick up the new topic pattern (a brief consumption pause). If no annotations are found, it falls back to `firefly.eda.consumer.kafka.default.topics`.
5. **Envelope flow** — inbound records are wrapped in the unified `EventEnvelope`, emitted to a reactive `Sinks.Many` stream, and dispatched through `EventListenerProcessor` to your handlers.

## Documentation

- Firefly Framework documentation hub and module catalog: [github.com/fireflyframework](https://github.com/fireflyframework)
- EDA core (ports, annotations, `EdaProperties`): [`fireflyframework-eda`](https://github.com/fireflyframework/fireflyframework-eda)
- Sibling transports: [`fireflyframework-eda-rabbitmq`](https://github.com/fireflyframework/fireflyframework-eda-rabbitmq), [`fireflyframework-eda-postgres`](https://github.com/fireflyframework/fireflyframework-eda-postgres)

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
