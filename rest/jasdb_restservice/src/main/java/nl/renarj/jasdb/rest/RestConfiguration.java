package nl.renarj.jasdb.rest;

import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.core.type.AnnotatedTypeMetadata;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Conditional(RestConfiguration.Condition.class)
@Import({WebMvcAutoConfiguration.class, EmbeddedServletContainerAutoConfiguration.class})
@ComponentScan
public class RestConfiguration {
    private static final Logger LOG = getLogger(RestConfiguration.class);



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
