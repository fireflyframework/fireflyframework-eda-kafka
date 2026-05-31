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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for Kafka Consumer infrastructure.
 * <p>
 * This configuration creates Kafka consumer beans when:
 * <ul>
 *   <li>Kafka classes are available on classpath</li>
 *   <li>Consumers are globally enabled (firefly.eda.consumer.enabled=true)</li>
 *   <li>Kafka consumer is enabled (firefly.eda.consumer.kafka.default.enabled=true)</li>
 *   <li>Bootstrap servers are configured (firefly.eda.consumer.kafka.default.bootstrap-servers)</li>
 * </ul>
 * <p>
 * <strong>Configuration Source:</strong> firefly.eda.consumer.kafka.default.*
 * <p>
 * <strong>NOT using:</strong> spring.kafka.* properties (those are IGNORED)
 */
@Slf4j
@AutoConfiguration(after = FireflyEdaAutoConfiguration.class)
@ConditionalOnClass(name = "org.springframework.kafka.core.ConsumerFactory")
@ConditionalOnProperty(prefix = "firefly.eda", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(EdaProperties.class)
public class FireflyEdaKafkaConsumerAutoConfiguration {

    public FireflyEdaKafkaConsumerAutoConfiguration(EdaProperties props) {
        // Only log if consumers are enabled and Kafka is configured
        if (props.getConsumer().isEnabled()) {
            var kafkaConsumer = props.getConsumer().getKafka().get("default");
            if (kafkaConsumer != null && kafkaConsumer.isEnabled() &&
                kafkaConsumer.getBootstrapServers() != null && !kafkaConsumer.getBootstrapServers().isEmpty()) {
                log.info("--------------------------------------------------------------------------------");
                log.info("FIREFLY EDA KAFKA CONSUMER - INITIALIZING");
                log.info("--------------------------------------------------------------------------------");
            } else {
                log.debug("Firefly EDA Kafka Consumer auto-configuration loaded but not creating beans (disabled or not configured)");
            }
        } else {
            log.debug("Firefly EDA Kafka Consumer auto-configuration loaded but not creating beans (consumers globally disabled)");
        }
    }

    /**
     * Creates a Kafka ConsumerFactory from Firefly EDA properties when:
     * - Consumer is globally enabled
     * - Kafka consumer is enabled (defaults to true)
     * - Bootstrap servers are configured in firefly.eda.consumer.kafka.default.bootstrap-servers
     *
     * <p><strong>Configuration Source:</strong> firefly.eda.consumer.kafka.default.*
     * <p><strong>NOT using:</strong> spring.kafka.* properties (those are IGNORED)
     */
    @Bean(name = "fireflyEdaKafkaConsumerFactory")
    @Primary
    @ConditionalOnMissingBean(name = "fireflyEdaKafkaConsumerFactory")
    @ConditionalOnExpression("${firefly.eda.consumer.enabled:false} && ${firefly.eda.consumer.kafka.default.enabled:false} && '${firefly.eda.consumer.kafka.default.bootstrap-servers:}'.length() > 0")
    public ConsumerFactory<String, Object> fireflyEdaKafkaConsumerFactory(EdaProperties props) {
        log.info("Creating Kafka ConsumerFactory from firefly.eda.consumer.kafka.default.* properties");

        EdaProperties.Consumer.KafkaConfig kafkaProps = props.getConsumer().getKafka().get("default");

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getBootstrapServers());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.getConsumer().getGroupId());
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProps.getAutoOffsetReset() != null ?
            kafkaProps.getAutoOffsetReset() : "earliest");

        log.info("  - Bootstrap servers: {}", kafkaProps.getBootstrapServers());
        log.info("  - Consumer group ID: {}", props.getConsumer().getGroupId());
        log.info("  - Auto offset reset: {}", kafkaProps.getAutoOffsetReset() != null ? kafkaProps.getAutoOffsetReset() : "earliest");

        // Set deserializers
        String keyDeserializer = kafkaProps.getKeyDeserializer() != null ?
            kafkaProps.getKeyDeserializer() : StringDeserializer.class.getName();
        String valueDeserializer = kafkaProps.getValueDeserializer() != null ?
            kafkaProps.getValueDeserializer() : StringDeserializer.class.getName();

        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        log.info("  - Key deserializer: {}", keyDeserializer);
        log.info("  - Value deserializer: {}", valueDeserializer);

        // Add any additional properties
        if (kafkaProps.getProperties() != null && !kafkaProps.getProperties().isEmpty()) {
            log.info("  - Additional properties: {}", kafkaProps.getProperties().keySet());
            configProps.putAll(kafkaProps.getProperties());
        }

        DefaultKafkaConsumerFactory<String, Object> factory = new DefaultKafkaConsumerFactory<>(configProps);
        log.info("Kafka ConsumerFactory created successfully");
        return factory;
    }

    /**
     * Creates a Kafka listener container factory from Firefly-created ConsumerFactory when:
     * - Kafka classes are available on classpath
     * - No existing kafkaListenerContainerFactory bean with this name exists
     * - Firefly EDA ConsumerFactory is available
     *
     * <p><strong>Uses:</strong> fireflyEdaKafkaConsumerFactory bean
     */
    @Bean(name = "fireflyEdaKafkaListenerContainerFactory")
    @Primary
    @ConditionalOnMissingBean(name = "fireflyEdaKafkaListenerContainerFactory")
    @ConditionalOnBean(name = "fireflyEdaKafkaConsumerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> fireflyEdaKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> fireflyEdaKafkaConsumerFactory) {
        log.info("Creating Kafka listener container factory from fireflyEdaKafkaConsumerFactory");
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(fireflyEdaKafkaConsumerFactory);
        log.info("Kafka Consumer infrastructure created successfully");
        log.info("--------------------------------------------------------------------------------");
        return factory;
    }
}

