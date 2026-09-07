package com.greyorange.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Demo app: wires L1 (RackEntity/RackRepository), L2 (WorkflowSagaBase sagas)
 * and L3 (AbstractJobWorker + @JobWorker) to a real Zeebe broker for the
 * pick_demo_rack_pptl.bpmn process (rack_arrived -> master_pptl_press).
 */
@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
