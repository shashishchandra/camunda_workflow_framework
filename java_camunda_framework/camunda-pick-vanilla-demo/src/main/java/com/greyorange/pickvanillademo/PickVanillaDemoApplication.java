package com.greyorange.pickvanillademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Demo app: mirrors butler_server's pick_front_hsm vanilla (bin-agent) flow
 * in a Camunda 8 diagram for demonstration purposes. This app never
 * interacts with the real HSM: it independently consumes the real
 * rack-arrival Kafka topic butler_server already publishes (its own
 * consumer group, zero butler_server changes), every other step is
 * triggered by hand-published demo-only Kafka messages, and all data comes
 * from ButlerServerApiClient calling the new, read-only
 * pick_vanilla_demo_http_handler (never a write). See README.md.
 */
@SpringBootApplication
public class PickVanillaDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(PickVanillaDemoApplication.class, args);
    }
}
