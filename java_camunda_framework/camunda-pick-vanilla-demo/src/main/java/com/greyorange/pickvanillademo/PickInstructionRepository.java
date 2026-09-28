package com.greyorange.pickvanillademo;

import com.greyorange.camunda.l1.WorkflowStatus;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PickInstructionRepository extends JpaRepository<PickInstructionEntity, String> {

    /** The instruction currently being worked for this rack, if any. */
    Optional<PickInstructionEntity> findFirstByPpsIdAndStatusOrderByCreatedAtDesc(String ppsId, WorkflowStatus status);
}
