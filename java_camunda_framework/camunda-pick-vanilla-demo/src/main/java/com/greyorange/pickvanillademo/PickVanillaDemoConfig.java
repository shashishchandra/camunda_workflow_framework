package com.greyorange.pickvanillademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.greyorange.camunda.l2.WorkflowRuleEngine;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.web.client.RestClient;

/**
 * WorkflowSagaBase (L2) takes a plain {@link EntityManager} constructor argument rather
 * than using field-level {@code @PersistenceContext} injection. A shared/transactional
 * proxy is required here so checkpoint writes join the caller's {@code @Transactional}
 * boundary instead of opening a second, disconnected persistence context.
 */
@Configuration
public class PickVanillaDemoConfig {

    @Bean
    public EntityManager entityManager(EntityManagerFactory emf) {
        return SharedEntityManagerCreator.createSharedEntityManager(emf);
    }

    /**
     * Boot 4.1.1's own JacksonAutoConfiguration targets the new Jackson 3
     * (tools.jackson.databind) API; it doesn't register a classic
     * com.fasterxml.jackson.databind.ObjectMapper bean, which is what the
     * inbound connectors (and the rest of the Connector SDK) expect.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public WorkflowRuleEngine workflowRuleEngine() {
        return new WorkflowRuleEngine();
    }

    @Bean
    public RestClient butlerServerRestClient(@Value("${butler-server.base-url}") String baseUrl) {
        return RestClient.builder().baseUrl(baseUrl).build();
    }
}
