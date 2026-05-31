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

package org.fireflyframework.eda.config;

import org.fireflyframework.eda.properties.EdaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for Kafka Publisher infrastructure.
 * <p>
 * This configuration creates Kafka producer beans when:
 * <ul>
 *   <li>Kafka classes are available on classpath</li>
 *   <li>Publishers are globally enabled (firefly.eda.publishers.enabled=true)</li>
 *   <li>Kafka publisher is enabled (firefly.eda.publishers.kafka.default.enabled=true)</li>
 *   <li>Bootstrap servers are configured (firefly.eda.publishers.kafka.default.bootstrap-servers)</li>
 * </ul>
 * <p>
 * <strong>Configuration Source:</strong> firefly.eda.publishers.kafka.default.*
 * <p>
 * <strong>NOT using:</strong> spring.kafka.* properties (those are IGNORED)
 */
@Slf4j
@AutoConfiguration(after = FireflyEdaAutoConfiguration.class)
@ConditionalOnClass(name = "org.springframework.kafka.core.KafkaTemplate")
@ConditionalOnProperty(prefix = "firefly.eda", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(EdaProperties.class)
public class FireflyEdaKafkaPublisherAutoConfiguration {

    public FireflyEdaKafkaPublisherAutoConfiguration(EdaProperties props) {
        // Only log if publishers are enabled and Kafka is configured
        if (props.getPublishers().isEnabled()) {
            var kafkaPublisher = props.getPublishers().getKafka().get("default");
            if (kafkaPublisher != null && kafkaPublisher.isEnabled() &&
                kafkaPublisher.getBootstrapServers() != null && !kafkaPublisher.getBootstrapServers().isEmpty()) {
                log.info("--------------------------------------------------------------------------------");
                log.info("FIREFLY EDA KAFKA PUBLISHER - INITIALIZING");
                log.info("--------------------------------------------------------------------------------");
            } else {
                log.debug("Firefly EDA Kafka Publisher auto-configuration loaded but not creating beans (disabled or not configured)");
            }
        } else {
            log.debug("Firefly EDA Kafka Publisher auto-configuration loaded but not creating beans (publishers globally disabled)");
        }
    }

    /**
     * Creates a Kafka ProducerFactory from Firefly EDA properties when:
     * - Publishers are globally enabled
     * - Kafka publisher is enabled (defaults to true)
     * - Bootstrap servers are configured in firefly.eda.publishers.kafka.default.bootstrap-servers
     *
     * <p><strong>Configuration Source:</strong> firefly.eda.publishers.kafka.default.*
     * <p><strong>NOT using:</strong> spring.kafka.* properties (those are IGNORED)
     */
    @Bean(name = "fireflyEdaKafkaProducerFactory")
    @ConditionalOnMissingBean(name = "fireflyEdaKafkaProducerFactory")
    @ConditionalOnExpression("${firefly.eda.publishers.enabled:false} && ${firefly.eda.publishers.kafka.default.enabled:false} && '${firefly.eda.publishers.kafka.default.bootstrap-servers:}'.length() > 0")
    public ProducerFactory<String, Object> fireflyEdaKafkaProducerFactory(EdaProperties props) {
        log.info("Creating Kafka ProducerFactory from firefly.eda.publishers.kafka.default.* properties");

        EdaProperties.Publishers.KafkaConfig kafkaProps = props.getPublishers().getKafka().get("default");

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getBootstrapServers());
        log.info("  - Bootstrap servers: {}", kafkaProps.getBootstrapServers());

        // Set serializers
        String keySerializer = kafkaProps.getKeySerializer() != null ?
            kafkaProps.getKeySerializer() : StringSerializer.class.getName();
        String valueSerializer = kafkaProps.getValueSerializer() != null ?
            kafkaProps.getValueSerializer() : StringSerializer.class.getName();

        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        log.info("  - Key serializer: {}", keySerializer);
        log.info("  - Value serializer: {}", valueSerializer);

        // Add any additional properties
        if (kafkaProps.getProperties() != null && !kafkaProps.getProperties().isEmpty()) {
            log.info("  - Additional properties: {}", kafkaProps.getProperties().keySet());
            configProps.putAll(kafkaProps.getProperties());
        }

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(configProps);
        log.info("Kafka ProducerFactory created successfully");
        return factory;
    }

    /**
     * Creates a KafkaTemplate from Firefly-created ProducerFactory when:
     * - Kafka classes are available on classpath
     * - No existing KafkaTemplate bean with this name exists
     * - Firefly EDA ProducerFactory is available
     *
     * <p><strong>Uses:</strong> fireflyEdaKafkaProducerFactory bean
     */
    @Bean(name = "fireflyEdaKafkaTemplate")
    @ConditionalOnMissingBean(name = "fireflyEdaKafkaTemplate")
    @ConditionalOnBean(name = "fireflyEdaKafkaProducerFactory")
    public KafkaTemplate<String, Object> fireflyEdaKafkaTemplate(ProducerFactory<String, Object> fireflyEdaKafkaProducerFactory) {
        log.info("Creating KafkaTemplate from fireflyEdaKafkaProducerFactory");
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(fireflyEdaKafkaProducerFactory);
        log.info("Kafka Publisher infrastructure created successfully");
        log.info("--------------------------------------------------------------------------------");
        return template;
    }
}

