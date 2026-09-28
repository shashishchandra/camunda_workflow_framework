package com.greyorange.pickvanillademo;

/**
 * Rack-level phases only -- everything a whole rack cycle passes through ONCE,
 * regardless of how many pick instructions the outer loop processes in between.
 * The cycle stays in PROCESSING_PICKLIST for the entire duration of that loop; see
 * PickInstructionPhase for the per-pick-instruction sequence that resets fresh on
 * every loop iteration, mirroring the real HSM's own two-level structure (the outer
 * loop cycles back to waiting_for_pick_start_node with local state wiped via
 * get_resetted_hsm_state_v2/1, while pps_manager tracks whether more pick
 * instructions remain for the rack).
 */
public enum PickCyclePhase {
    WAIT_FOR_SOURCE_ARRIVAL,
    PROCESSING_PICKLIST,
    AUTO_DEST_CLEAR,
    RACK_RELEASED
}
