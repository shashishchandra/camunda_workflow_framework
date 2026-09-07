package com.greyorange.camunda.l1;

/**
 * L1 · Data Access Layer (DAL)
 *
 * Generic contract for fetching a raw DTO from any source and transforming it
 * into a domain entity. Implementations hide the source technology behind a
 * uniform fetch → validate → transform pipeline.
 *
 * TYPE PARAMS:
 *   D — the raw DTO produced by this source (must implement WorkflowDto)
 *   E — the domain entity this DAL produces (must extend WorkflowEntityBase)
 *
 * SOURCES: DB · HTTP · Kafka · InfluxDB · File  (declared via source())
 *
 * USAGE:
 *
 *   @Component
 *   public class ScanEventDal implements WorkflowDal<ScanEventDto, PickFrontEntity> {
 *
 *       @Override public DalSource source() { return DalSource.KAFKA; }
 *
 *       @Override
 *       public ScanEventDto fetch(String id) {
 *           // consume from Kafka or pull from local store
 *           return kafkaStore.get(id);
 *       }
 *
 *       @Override
 *       public PickFrontEntity toEntity(ScanEventDto dto) {
 *           validateMandatory(dto);           // checks @Mandatory fields
 *           PickFrontEntity e = new PickFrontEntity();
 *           e.setOrderId(dto.orderId);
 *           e.setBarcode(dto.barcode);
 *           e.setContainerId(dto.containerId); // optional — may be null
 *           return e;
 *       }
 *   }
 *
 * CONVENIENCE:
 *   The default load(id) chains fetch → toEntity in one call.
 *   Override if the two steps need to be decoupled (e.g. caching the DTO).
 */
public interface WorkflowDal<D extends WorkflowDto, E extends WorkflowEntityBase> {

    /** Declares where this DAL reads from. */
    DalSource source();

    /**
     * Fetch raw data from the source by id (orderId, correlationId, filename, etc.).
     * Must not apply business logic — pure I/O.
     */
    D fetch(String id);

    /**
     * Transform a raw DTO into a domain entity.
     * Must validate all @Mandatory fields and throw IllegalArgumentException on missing values.
     * Must not perform I/O.
     */
    E toEntity(D dto);

    /**
     * Convenience: fetch then transform.
     * Override to inject caching or intermediate enrichment.
     */
    default E load(String id) {
        return toEntity(fetch(id));
    }

    /**
     * Validate that all fields annotated @WorkflowDto.Mandatory on the DTO are non-null/non-blank.
     * Call this at the start of toEntity().
     */
    default void validateMandatory(D dto) {
        for (var field : dto.getClass().getDeclaredFields()) {
            if (!field.isAnnotationPresent(WorkflowDto.Mandatory.class)) continue;
            field.setAccessible(true);
            try {
                Object val = field.get(dto);
                if (val == null || (val instanceof String s && s.isBlank())) {
                    String reason = field.getAnnotation(WorkflowDto.Mandatory.class).reason();
                    throw new IllegalArgumentException(
                        "Mandatory field '" + field.getName() + "' is missing on "
                        + dto.getClass().getSimpleName()
                        + (reason.isBlank() ? "" : " — " + reason));
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot inspect field " + field.getName(), e);
            }
        }
    }
}
