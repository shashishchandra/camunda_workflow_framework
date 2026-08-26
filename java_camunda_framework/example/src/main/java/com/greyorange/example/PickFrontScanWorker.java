package com.greyorange.example;

import com.greyorange.camunda.l3.AbstractJobWorker;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import org.springframework.stereotype.Component;

/**
 * L3 example — pick_front scan worker using camunda-l3-starter.
 *
 * type MUST match <zeebe:taskDefinition type="pick.front.process_scan"/> in BPMN.
 */
@Component
public class PickFrontScanWorker extends AbstractJobWorker {

    private final PickFrontSaga saga;

    public PickFrontScanWorker(PickFrontSaga saga) {
        this.saga = saga;
    }

    @JobWorker(type = "pick.front.process_scan", autoComplete = false)
    public void handle(JobClient client, ActivatedJob job,
                       @Variable String orderId,
                       @Variable String ppsId,
                       @Variable String scanResult) {
        execute(client, job, () ->
            saga.commitScan(orderId, ppsId, scanResult)
        );
    }
}
