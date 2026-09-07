package com.greyorange.demo;

import com.greyorange.camunda.l2.WorkflowMq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * L2's WorkflowMq is broker-agnostic by design (no KafkaMq/RabbitMq shipped in the
 * framework yet). For this demo we just log the post-commit publish so it's visible
 * in the console alongside Operate — swap for a real KafkaMq in a production service.
 */
@Component
public class DemoLoggingMq implements WorkflowMq {

    private static final Logger log = LoggerFactory.getLogger(DemoLoggingMq.class);

    @Override
    public void publish(String destination, Object payload) {
        log.info(">>> MQ PUBLISH (post-commit)  topic={}  payload={}", destination, payload);
    }
}
