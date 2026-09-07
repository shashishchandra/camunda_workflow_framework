package com.greyorange.camunda.l1;

import java.lang.annotation.*;

/**
 * L1 · DTO Base
 *
 * Marker interface for all raw data transfer objects produced by a WorkflowDal.
 * DTOs are inert data bags — no business logic, no JPA annotations.
 * They exist only to carry raw source data before transformation into an entity.
 *
 * FIELD CONVENTION:
 *   Mandatory fields → annotate with @WorkflowDto.Mandatory
 *   Optional fields  → annotate with @WorkflowDto.Optional (or leave unannotated)
 *
 * EXAMPLE:
 *
 *   public class ScanEventDto implements WorkflowDto {
 *
 *       @WorkflowDto.Mandatory
 *       public String orderId;
 *
 *       @WorkflowDto.Mandatory
 *       public String barcode;
 *
 *       @WorkflowDto.Optional
 *       public String containerId;   // not required for all scan types
 *   }
 *
 * The WorkflowDal.toEntity() implementation is responsible for validating
 * mandatory fields and throwing on missing values before entity construction.
 */
public interface WorkflowDto {

    /**
     * Marks a DTO field as mandatory.
     * The DAL must reject the DTO if this field is null or blank.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    @Documented
    @interface Mandatory {
        String reason() default "";
    }

    /**
     * Marks a DTO field as optional.
     * The DAL may apply a default or skip it during entity construction.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    @Documented
    @interface Optional {
        String defaultValue() default "";
    }
}
