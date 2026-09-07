package com.greyorange.camunda.l3;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * L3 Orchestration — abstract base for all Zeebe job workers.
 *
 * KEY INVARIANTS:
 *   1. Workers POLL Zeebe via gRPC long-poll — Zeebe does NOT push.
 *   2. The @JobWorker(type="...") annotation value MUST match
 *      <zeebe:taskDefinition type="..."/> in the BPMN exactly.
 *      No other registration or mapping is needed.
 *   3. Use autoComplete=false and call complete()/fail() explicitly
 *      so failures are reported back to Zeebe with the right retry count.
 *
 * USAGE: extend and annotate with @JobWorker:
 *
 *   @Component
 *   public class PickFrontScanWorker extends AbstractJobWorker {
 *
 *       private final PickFrontSaga saga;
 *
 *       @JobWorker(type = "pick.front.process_scan", autoComplete = false)
 *       public void handle(JobClient client, ActivatedJob job,
 *                          @Variable String orderId,
 *                          @Variable String scanResult) {
 *           execute(client, job, () ->
 *               saga.commitScan(orderId, scanResult)
 *           );
 *       }
 *   }
 */
public abstract class AbstractJobWorker {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Execute the worker logic, auto-completing or failing the job.
     * @param client  Zeebe job client
     * @param job     the activated job
     * @param work    lambda that contains the business logic (calls L2 saga)
     */
    protected void execute(JobClient client, ActivatedJob job, WorkerAction work) {
        try {
            Map<String, Object> outVars = work.run();
            client.newCompleteCommand(job.getKey())
                  .variables(outVars != null ? outVars : Map.of())
                  .send()
                  .join();
        } catch (Exception e) {
            log.error("Job {} failed: {}", job.getType(), e.getMessage(), e);
            client.newFailCommand(job.getKey())
                  .retries(job.getRetries() - 1)
                  .errorMessage(e.getMessage())
                  .send()
                  .join();
        }
    }

    @FunctionalInterface
    public interface WorkerAction {
        /** Return output variables to set on Zeebe process, or null for none. */
        Map<String, Object> run() throws Exception;
    }
}


//l4 -> models for screen_id from l3, Screen data object to L3 and vice versa