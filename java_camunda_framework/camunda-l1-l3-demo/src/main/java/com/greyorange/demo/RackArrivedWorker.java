package com.greyorange.demo;

import com.greyorange.camunda.l1.WorkflowContext;
import com.greyorange.camunda.l3.AbstractJobWorker;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import org.springframework.stereotype.Component;

/**
 * L3 — Zeebe job worker for the "Rack Arrived" service task.
 * type MUST match <zeebe:taskDefinition type="pick.demo.rack_arrived"/> in the BPMN.
 */
@Component
public class RackArrivedWorker extends AbstractJobWorker {

    private final RackArrivedSaga saga;

    public RackArrivedWorker(RackArrivedSaga saga) {
        this.saga = saga;
    }

    @JobWorker(type = "pick.demo.rack_arrived", autoComplete = false)
    public void handle(JobClient client, ActivatedJob job, @Variable String workOrderId) {
        WorkflowContext.set(WorkflowContext.of(job.getVariablesAsMap()));
        try {
            execute(client, job, () -> saga.markArrived(workOrderId));
        } finally {
            WorkflowContext.clear();
        }
    }
}
