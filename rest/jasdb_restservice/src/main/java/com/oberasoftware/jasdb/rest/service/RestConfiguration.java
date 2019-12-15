package com.oberasoftware.jasdb.rest.service;

import org.slf4j.Logger;
import org.springframework.context.annotation.*;
import org.springframework.core.type.AnnotatedTypeMetadata;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Conditional(RestConfiguration.Condition.class)
@ComponentScan
public class RestConfiguration {
    static class Condition implements ConfigurationCondition {

        @Override
        public ConfigurationPhase getConfigurationPhase() {
            return ConfigurationPhase.PARSE_CONFIGURATION;
        }

        @Override
        public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
            return RestConfigurationLoader.isEnabled();
        }
    }
}
