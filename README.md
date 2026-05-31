# Firefly Framework - EDA - Kafka

[![CI](https://github.com/fireflyframework/fireflyframework-eda-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-eda-kafka/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> Apache Kafka transport adapter for the Firefly EDA abstraction, providing reactive event publishing and consumption over Kafka.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

Firefly Framework EDA Kafka implements the `EventPublisher` and `EventConsumer` ports of the Firefly EDA core (`fireflyframework-eda`) using Apache Kafka as the underlying transport. It provides reactive, non-blocking publishing through `KafkaEventPublisher` and dynamic topic consumption through `KafkaEventConsumer`.

The adapter is fully driven by `firefly.eda.*` properties (Spring Kafka's native `spring.kafka.*` properties are intentionally ignored), keeping the configuration surface consistent across every Firefly EDA transport. Producer and consumer infrastructure beans are created by `FireflyEdaKafkaPublisherAutoConfiguration` and `FireflyEdaKafkaConsumerAutoConfiguration`, while the `KafkaEventPublisher` and `KafkaEventConsumer` components are discovered by the core component scan.

A `FireflyEdaKafkaEnvironmentPostProcessor` conditionally excludes Spring Boot's `KafkaAutoConfiguration` when the Kafka transport is not enabled, preventing spurious connection attempts on startup.

## Features

- Reactive Kafka `EventPublisher` implementation (`KafkaEventPublisher`)
- Dynamic, annotation-driven Kafka `EventConsumer` (`KafkaEventConsumer`)
- 100% hexagonal configuration via `firefly.eda.*` (no `spring.kafka.*` coupling)
- Firefly-owned `ProducerFactory` / `KafkaTemplate` and `ConsumerFactory` / listener container factory beans
- Conditional exclusion of Spring Boot `KafkaAutoConfiguration` when Kafka is disabled
- Header propagation and partitioning support
- Connection health monitoring
- Spring Boot auto-configuration via `AutoConfiguration.imports`
- Configurable via `firefly.eda.publishers.kafka.*` and `firefly.eda.consumer.kafka.*`

## Requirements

- Java 21+
- Spring Boot 3.x
- Maven 3.9+
- Apache Kafka broker

## Installation

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-eda-kafka</artifactId>
    <version>26.05.07</version>
</dependency>
```

## Quick Start

```xml
<dependencies>
    <dependency>
        <groupId>org.fireflyframework</groupId>
        <artifactId>fireflyframework-eda</artifactId>
    </dependency>
    <dependency>
        <groupId>org.fireflyframework</groupId>
        <artifactId>fireflyframework-eda-kafka</artifactId>
    </dependency>
</dependencies>
```

## Configuration

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

## Documentation

No additional documentation available for this project.

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
