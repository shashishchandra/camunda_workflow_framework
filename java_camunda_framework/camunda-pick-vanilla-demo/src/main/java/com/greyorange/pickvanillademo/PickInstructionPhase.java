package com.greyorange.pickvanillademo;

/**
 * Every node of ONE pick instruction's pass through the real pick-front vanilla HSM --
 * everything between the rack-level PROCESSING_PICKLIST phase (see PickCyclePhase) and
 * the outer loop's "any more pick instructions?" check. A fresh PickInstructionEntity
 * row (and therefore a fresh instance of this enum's progression) is created for each
 * pick instruction the outer loop processes, mirroring the real HSM's own
 * get_resetted_hsm_state_v2/1 -- it resets local state and re-enters near the top for
 * every instruction rather than trying to keep a single entity monotonically advancing
 * across all of them.
 */
public enum PickInstructionPhase {
    WAITING_FOR_CARRIER_VALIDATION,
    WAIT_FOR_WORK_ALLOCATION,
    WAIT_FOR_WORK_ALLOCATION_VANILLA_PPS,
    WAIT_FOR_DEST_CLEAR,
    WAIT_FOR_TOTE_ATTACHMENT,
    PICK_PRE_REQUISITES,
    WAITING_FOR_DEST_AGENT,
    PICKLIST_PROCESSING,
    PICKLIST_PROCESSING_VANILLA_PPS,
    FILTER_PICKLIST,
    ENTITY_TRANSPORT,
    SOURCE_OPERATIONS,
    EARLY_DISPLAY_ITEM_SCAN,
    SLOT_PICKING,
    WAIT_FOR_ENTITY_SCAN,
    POST_SCAN,
    WAIT_FOR_ENTITY_OPERATIONS,
    WAIT_FOR_PICK_CONFIRM,
    WAIT_FOR_PICK_BIN_CONFIRM,
    COMMIT_PPTL_PRESS,
    WAIT_FOR_PRINT,
    WAIT_FOR_PRINTOUT_CONFIRM,
    VALIDATE_AND_PROCESS_NEXT
}
