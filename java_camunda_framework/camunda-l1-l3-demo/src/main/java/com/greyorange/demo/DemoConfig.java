package com.greyorange.demo;

import com.greyorange.camunda.l2.WorkflowRuleEngine;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.SharedEntityManagerCreator;

/**
 * WorkflowSagaBase (L2) takes a plain {@link EntityManager} constructor argument rather
 * than using field-level {@code @PersistenceContext} injection. A shared/transactional
 * proxy is required here so checkpoint writes join the caller's {@code @Transactional}
 * boundary instead of opening a second, disconnected persistence context.
 */
@Configuration
public class DemoConfig {

    @Bean
    public EntityManager entityManager(EntityManagerFactory emf) {
        return SharedEntityManagerCreator.createSharedEntityManager(emf);
    }

    @Bean
    public WorkflowRuleEngine workflowRuleEngine() {
        return new WorkflowRuleEngine();
    }
}
