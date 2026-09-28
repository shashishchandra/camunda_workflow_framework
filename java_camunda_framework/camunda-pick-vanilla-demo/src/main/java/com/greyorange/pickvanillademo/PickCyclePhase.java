package com.greyorange.pickvanillademo;

/**
 * Every node of the pick-front vanilla HSM chain, in the order the HSM reaches them. Branch
 * alternatives (WAIT_FOR_DEST_CLEAR vs WAIT_FOR_TOTE_ATTACHMENT;
 * EARLY_DISPLAY_ITEM_SCAN vs SLOT_PICKING vs WAIT_FOR_ENTITY_SCAN;
 * WAIT_FOR_PRINTOUT_CONFIRM's presence or absence) only one of which fires
 * per instance -- see PickCycleSaga.advancePhase's explicit
 * `minimumPriorPhase` parameter for how the monotonic-advance check handles
 * this without assuming a strict `ordinal() - 1` predecessor.
 */
public enum PickCyclePhase {
    WAIT_FOR_SOURCE_ARRIVAL,
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
    VALIDATE_AND_PROCESS_NEXT,
    AUTO_DEST_CLEAR,
    RACK_RELEASED
}
