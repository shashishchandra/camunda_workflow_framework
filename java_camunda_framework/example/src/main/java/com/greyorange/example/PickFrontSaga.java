package com.greyorange.example;

import com.greyorange.camunda.l2.WorkflowSagaBase;
import com.greyorange.camunda.l1.WorkflowStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Map;

/**
 * L2 example — pick_front saga using camunda-l2-starter.
 * Swap PickFrontEntity / PickFrontRepository for your domain.
 */
@Service
public class PickFrontSaga extends WorkflowSagaBase {

    private final PickFrontRepository repository;

    public PickFrontSaga(KafkaTemplate<String, Object> kafka, PickFrontRepository repository) {
        super(kafka);
        this.repository = repository;
    }

    @Transactional
    public Map<String, Object> commitScan(String orderId, String ppsId, String scanResult) {
        return runSaga(
            () -> {
                var entity = repository.findByOrderId(orderId).orElseThrow();
                entity.setScanResult(scanResult);
                entity.setStatus(WorkflowStatus.COMPLETED);
                repository.save(entity);
                return Map.of(
                    "type",        "pick.front.scanned",
                    "order_id",    orderId,
                    "pps_id",      ppsId,
                    "scan_result", scanResult
                );
            },
            "pick.events"
        );
    }
}
