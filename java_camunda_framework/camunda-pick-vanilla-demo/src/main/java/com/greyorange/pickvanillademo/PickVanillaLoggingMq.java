package com.greyorange.pickvanillademo;

import com.greyorange.camunda.l2.WorkflowMq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * L2's WorkflowMq is broker-agnostic by design (no KafkaMq ships with the
 * framework yet). This app's real orchestration signal is Zeebe's own state
 * (this IS the process now), so the saga's post-commit publish only needs
 * to be visible for local debugging.
 */
@Component
public class PickVanillaLoggingMq implements WorkflowMq {

    private static final Logger log = LoggerFactory.getLogger(PickVanillaLoggingMq.class);

    @Override
    public void publish(String destination, Object payload) {
        log.info(">>> MQ PUBLISH (post-commit)  topic={}  payload={}", destination, payload);
    }
}
