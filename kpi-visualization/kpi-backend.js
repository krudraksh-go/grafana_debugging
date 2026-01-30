/**
 * KPI Backend Query Definitions
 * =============================
 * 
 * IMPORTANT: All queries are STORED but NOT EXECUTED.
 * This file contains the query definitions for all KPIs in UPH.yaml.
 * DB connection path to be configured later.
 * 
 * Status: STUB MODE - No actual database calls are made
 */

const KPI_QUERIES = {
    // ========================================
    // PICKS PER TOTE SECTION
    // ========================================
    
    orderlinesPerTote: {
        id: "orderlines_per_tote",
        name: "Orderlines per tote",
        database: "GreyOrange",
        table: "item_picked",
        columns: ["num_of_orderlines", "tote_id"],
        query: `SELECT mean(num_of_orderlines) 
                FROM "GreyOrange"."autogen"."item_picked" 
                WHERE installation_id =~ /^$InstallationId$/ 
                AND time > $start AND time <= $end`,
        sourceFile: "src/orderline_transactions.py",
        enabled: false  // Set to true when DB connection is ready
    },

    quantityPerOrderline: {
        id: "quantity_per_orderline",
        name: "Quantity per orderline",
        database: "platform_srms",
        table: "stock_unit",
        columns: ["quantity", "station_type", "storage_type"],
        query: `SELECT station_type, storage_type, avg(stock_unit.quantity) 
                FROM service_request 
                JOIN service_request_children ON service_request.id = service_request_children.service_request_id 
                JOIN service_request_expectations ON service_request_children.servicerequests_id = service_request_expectations.service_request_id 
                JOIN container_stock_units ON service_request_expectations.expectations_id = container_stock_units.container_id 
                JOIN stock_unit ON container_stock_units.stockunits_id = stock_unit.id 
                WHERE service_request.status IN ('CREATED') 
                GROUP BY station_type, storage_type`,
        sourceFile: "src/system_order_stats_only_for_cloud_sites.py",
        dbType: "postgresql",
        enabled: false
    },

    // ========================================
    // OPERATOR PULL TPH SECTION
    // ========================================

    owtPerUnit: {
        id: "owt_per_unit",
        name: "OWT per unit",
        database: "GreyOrange",
        table: "ppstask_events",
        columns: ["value", "event", "task_type", "pps_id"],
        query: `SELECT mean(value)/1000 
                FROM "GreyOrange"."autogen"."ppstask_events" 
                WHERE installation_id =~ /^$InstallationId$/ 
                AND event='rack_started_to_depart_pps' 
                AND task_type='pick' 
                AND value<550000 
                AND pps_id =~ /^$PpsId$/ 
                AND time > $start AND time <= $end`,
        formula: "mean(value) / 1000 (convert ms to seconds)",
        sourceFile: "Grafana dashboard",
        enabled: false
    },

    ppf: {
        id: "ppf",
        name: "PPF (Picks Per Face)",
        database: "GreyOrange",
        table: "picks_per_rack_face",
        columns: ["total_picks_value", "total_picks", "pps_id", "rack", "face"],
        query: `SELECT mean(total_picks_value) 
                FROM "GreyOrange"."autogen"."picks_per_rack_face" 
                WHERE installation_id =~ /^$InstallationId$/ 
                AND pps_id =~ /^$PpsId$/ 
                AND time > $start AND time <= $end`,
        sourceFile: "src/pick_per_rack_face.py",
        enabled: false
    },

    loggedInPps: {
        id: "logged_in_pps",
        name: "# of logged in PPS",
        database: "GreyOrange",
        table: "pps_data",
        columns: ["value", "front_logged_in", "mode", "pps_id"],
        query: `SELECT count(value)*5 
                FROM "GreyOrange"."autogen"."pps_data" 
                WHERE installation_id =~ /^$InstallationId$/ 
                AND front_logged_in='true' 
                AND mode='pick' 
                AND pps_id =~ /^$PpsId$/ 
                AND time > $start AND time <= $end 
                GROUP BY time($TimeInterval)`,
        note: "Data recorded every 5 mins, hence *5",
        sourceFile: "Grafana dashboard",
        enabled: false
    },

    // ========================================
    // VTM SECTION
    // ========================================

    vtmTphAllVtms: {
        id: "vtm_tph_all_vtms",
        name: "VTM TPH all VTMs",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "ranger_type", "event"],
        query: `SELECT count(*) / (time_range_hours) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE ranger_type='vtm' 
                AND event='cycle_time' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    workingVtms: {
        id: "working_vtms",
        name: "# of working VTMs",
        database: "GreyOrange",
        table: "ranger_events",
        columns: ["ranger_id", "ranger_type", "event", "task_status"],
        query: `SELECT count(distinct ranger_id) 
                FROM "GreyOrange"."autogen"."ranger_events" 
                WHERE ranger_type='vtm' 
                AND event='task_completed' 
                AND time > $start AND time <= $end`,
        enabled: false
    },

    relayBlockedUtilization: {
        id: "relay_blocked_utilization",
        name: "Blocked due to relay position unavailability",
        database: "Alteryx",
        table: "relay_utilization",
        columns: ["dwell_time", "state", "aisle_id", "current_location"],
        query: `SELECT mean(dwell_time) 
                FROM "Alteryx"."autogen"."relay_utilization" 
                WHERE state='occupied' 
                AND time > $start AND time <= $end 
                GROUP BY aisle_id`,
        sourceFile: "src/relay_utilization.py",
        enabled: false
    },

    owt: {
        id: "owt",
        name: "OWT (Operator Working Time)",
        database: "airflow",
        table: "flow_transactions_sec_alteryx",
        columns: ["total_time", "quantity"],
        query: `SELECT mean(total_time) 
                FROM "airflow"."autogen"."flow_transactions_sec_alteryx" 
                WHERE time > $start AND time <= $end`,
        sourceFile: "src/Flow_transactions_sec_alteryx.py",
        enabled: false
    },

    cycleTimeVtm: {
        id: "cycle_time_vtm",
        name: "Cycle time (VTM)",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event", "ranger_type"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='cycle_time' 
                AND ranger_type='vtm' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    numberOfPps: {
        id: "number_of_pps",
        name: "Number of PPS",
        database: "GreyOrange",
        table: "pps_data",
        columns: ["pps_id", "mode"],
        query: `SELECT count(distinct pps_id) 
                FROM "GreyOrange"."autogen"."pps_data" 
                WHERE mode='pick' 
                AND time > $start AND time <= $end`,
        enabled: false
    },

    horizontalMovement: {
        id: "horizontal_movement",
        name: "Horizontal movement",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT sum(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event IN ('Time to Reach IO Point', 'Time to Reach PPS') 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    vtmNavFactor: {
        id: "vtm_nav_factor",
        name: "VTM nav factor",
        database: "Alteryx",
        table: "task_cycle_times_summary",
        columns: ["nav_factor", "ranger_type"],
        query: `SELECT mean(nav_factor) 
                FROM "Alteryx"."autogen"."task_cycle_times_summary" 
                WHERE ranger_type='vtm' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/consolidated_task_cycle_times.py",
        enabled: false
    },

    aisleChanges: {
        id: "aisle_changes",
        name: "% aisle changes",
        database: "Alteryx",
        table: "vtm_aisle_change",
        columns: ["aisle_id", "prev_aisle_id"],
        query: `SELECT count(aisle_change)/count(*)*100 
                FROM "Alteryx"."autogen"."vtm_aisle_change" 
                WHERE time > $start AND time <= $end`,
        sourceFile: "src/vtm_aisle_change.py",
        enabled: false
    },

    fetchDrop: {
        id: "fetch_drop",
        name: "Fetch / drop",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event", "action_type"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event IN ('Time to Lift a tote', 'Time to drop tote') 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    nDeepDigging: {
        id: "n_deep_digging",
        name: "% n-deep digging",
        database: "Alteryx",
        table: "relay_journey_raw",
        columns: ["current_location_depth"],
        query: `SELECT mean(current_location_depth) 
                FROM "Alteryx"."autogen"."relay_journey_raw" 
                WHERE current_location_depth > 0 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    storableToRelayTph: {
        id: "storable_to_relay_tph",
        name: "Storable to relay TPH",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT count(*) / (time_range_hours) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='storable_to_relay_point' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    relayToStorableTph: {
        id: "relay_to_storable_tph",
        name: "Relay to storable TPH",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT count(*) / (time_range_hours) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='relay_point_to_station' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    // ========================================
    // RELAY POSITION OCCUPANCY SECTION
    // ========================================

    avgRelayOccupancy: {
        id: "avg_relay_occupancy",
        name: "Avg. relay position occupancy",
        database: "Alteryx",
        table: "relay_utilization",
        columns: ["dwell_time", "state", "aisle_id", "current_location"],
        query: `SELECT mean(dwell_time) 
                FROM "Alteryx"."autogen"."relay_utilization" 
                WHERE state='occupied' 
                AND time > $start AND time <= $end 
                GROUP BY aisle_id`,
        sourceFile: "src/relay_utilization.py",
        enabled: false
    },

    // ========================================
    // HTM SECTION
    // ========================================

    htmTphAllHtms: {
        id: "htm_tph_all_htms",
        name: "HTM TPH all HTMs",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "ranger_type", "event"],
        query: `SELECT count(*) / (time_range_hours) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE ranger_type='htm' 
                AND event='cycle_time' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    workingHtms: {
        id: "working_htms",
        name: "# of working HTMs",
        database: "GreyOrange",
        table: "ranger_events",
        columns: ["ranger_id", "ranger_type", "event"],
        query: `SELECT count(distinct ranger_id) 
                FROM "GreyOrange"."autogen"."ranger_events" 
                WHERE ranger_type='htm' 
                AND event='task_completed' 
                AND time > $start AND time <= $end`,
        enabled: false
    },

    cycleTimeHtm: {
        id: "cycle_time_htm",
        name: "Cycle time (HTM)",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event", "ranger_type"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='cycle_time' 
                AND ranger_type='htm' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    htmNavFactor: {
        id: "htm_nav_factor",
        name: "Nav factor (HTM)",
        database: "Alteryx",
        table: "task_cycle_times_summary",
        columns: ["nav_factor", "ranger_type"],
        query: `SELECT mean(nav_factor) 
                FROM "Alteryx"."autogen"."task_cycle_times_summary" 
                WHERE ranger_type='htm' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/consolidated_task_cycle_times.py",
        enabled: false
    },

    pickFromRelay: {
        id: "pick_from_relay",
        name: "Pick from relay",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='tote_loaded' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    towardsPps: {
        id: "towards_pps",
        name: "Towards PPS",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='reached_pps' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    picking: {
        id: "picking",
        name: "Picking",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='going_pps_exit' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    backToStore: {
        id: "back_to_store",
        name: "Back to store",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='station_to_relay_point' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    dropOnRelay: {
        id: "drop_on_relay",
        name: "Drop on relay",
        database: "Alteryx",
        table: "relay_bot_journey",
        columns: ["time_diff", "event"],
        query: `SELECT mean(time_diff) 
                FROM "Alteryx"."autogen"."relay_bot_journey" 
                WHERE event='tote_unloaded' 
                AND time > $start AND time <= $end`,
        sourceFile: "src/relay_bot_journey.py",
        enabled: false
    },

    // ========================================
    // PUT / BACK TO STORE SECTION
    // ========================================

    putBackToStoreInputs: {
        id: "put_back_to_store_inputs",
        name: "Put / back to store inputs",
        database: "GreyOrange",
        table: "item_put",
        columns: ["uom_quantity_int", "pps_id", "station_type", "storage_type"],
        query: `SELECT sum(uom_quantity_int) 
                FROM "GreyOrange"."autogen"."item_put" 
                WHERE installation_id =~ /^$InstallationId$/ 
                AND time > $start AND time <= $end`,
        sourceFile: "src/put_uph.py",
        enabled: false
    }
};

// ========================================
// DB NOT FOUND KPIS
// ========================================
const DB_NOT_FOUND_KPIS = [
    { name: "Operator pull TPH", reason: "Derived metric - no direct DB table", parent: "TPH all PPS" },
    { name: "Effective VTMs", reason: "GMC internal calculation", parent: "VTM TPH all VTMs" },
    { name: "Work created", reason: "GMC internal state", parent: "Effective VTMs > Breakup #1" },
    { name: "Distribution of tasks", reason: "GMC internal", parent: "Work created" },
    { name: "Created task count", reason: "GMC internal", parent: "Work created" },
    { name: "Work assigned", reason: "GMC internal state", parent: "Breakup #1" },
    { name: "Bot error", reason: "No centralized error table", parent: "Work assigned" },
    { name: "# of healthy VTMs (non-error)", reason: "Inverse of error count, not tracked", parent: "Breakup #1" },
    { name: "L1 VTM TOR", reason: "Configuration value, not from DB", parent: "Breakup #2" },
    { name: "Distance", reason: "Derived from coordinates, not stored", parent: "Horizontal movement" },
    { name: "Vertical movement", reason: "Not tracked separately", parent: "Cycle time" },
    { name: "Mean vertical rack level traversed", reason: "Not stored in DB", parent: "Vertical movement" },
    { name: "% telescopic movement", reason: "Not tracked", parent: "Vertical movement" },
    { name: "Has tasks & towards PPS", reason: "Real-time GMC state", parent: "Avg relay position occupancy" },
    { name: "Doesn't have any task and eligible for back to store", reason: "Real-time GMC state", parent: "Avg relay position occupancy" },
    { name: "Variance of relay position occupancy", reason: "Statistical calculation", parent: "Avg relay position occupancy" },
    { name: "Work created variance", reason: "Statistical calculation", parent: "Variance of relay position occupancy" },
    { name: "Effective HTMs", reason: "GMC internal calculation", parent: "HTM TPH all HTMs" },
    { name: "Work created (HTM)", reason: "GMC internal", parent: "Effective HTMs > Breakup #1" },
    { name: "Count of 'Has tasks & towards PPS' totes on relay", reason: "Real-time GMC state", parent: "Work created (HTM)" },
    { name: "Blocked due to relay position unavailability because of back to store", reason: "Real-time GMC state", parent: "Work created (HTM)" },
    { name: "Distribution of 'Has tasks & towards PPS' totes on relay", reason: "Real-time GMC state", parent: "Work created (HTM)" },
    { name: "Work assigned (HTM)", reason: "GMC internal", parent: "Breakup #1" },
    { name: "Blocked due to bin promotion", reason: "Not tracked", parent: "Work assigned (HTM)" },
    { name: "Bot error (HTM)", reason: "No centralized table", parent: "Work assigned (HTM)" },
    { name: "# of healthy HTMs (non-error)", reason: "Not tracked", parent: "Breakup #1" },
    { name: "L1 HTM TOR", reason: "Configuration value", parent: "Breakup #2" },
    { name: "Nav factor on highways", reason: "Granular nav not tracked", parent: "Nav factor > Breakup #1" },
    { name: "Nav factor on racking", reason: "Granular nav not tracked", parent: "Nav factor > Breakup #1" },
    { name: "Dual cycle distance", reason: "Not stored", parent: "Cycle time > Navigation" },
    { name: "Current location to source", reason: "Real-time calculation", parent: "Cycle time > Breakup #2" },
    { name: "waiting in queue", reason: "Not tracked as separate metric", parent: "Cycle time > Breakup #2" },
    { name: "PPS to PPS", reason: "Hop events not aggregated", parent: "Cycle time > Pick" },
    { name: "Hopping %", reason: "Not tracked", parent: "PPS to PPS" },
    { name: "Network latency", reason: "No centralized latency database", parent: "Technical metrics" },
    { name: "GMC to VDA", reason: "Internal network metric", parent: "Network latency" },
    { name: "VDA to firmware", reason: "Internal network metric", parent: "Network latency" },
    { name: "Firmware to VDA", reason: "Internal network metric", parent: "Network latency" },
    { name: "VDA to GMC", reason: "Internal network metric", parent: "Network latency" }
];

// ========================================
// MATHEMATICAL FORMULAS
// ========================================
const KPI_FORMULAS = {
    uph: {
        name: "UPH (Units Per Hour)",
        formula: "UPH = (3600 × PPF) / (R2R_time + PPF × (total_OWT / items_picked))",
        variables: ["PPF", "R2R_time", "total_OWT", "items_picked"]
    },
    picksPerTote: {
        name: "Picks per tote",
        formula: "sum(uom_quantity_int) / count(distinct tote_id)",
        variables: ["uom_quantity_int", "tote_id"]
    },
    tphPerVtm: {
        name: "TPH / VTM",
        formula: "VTM_TPH_all_VTMs / Effective_VTMs",
        variables: ["VTM_TPH_all_VTMs", "Effective_VTMs"]
    },
    tphPerHtm: {
        name: "TPH / HTM",
        formula: "HTM_TPH_all_HTMs / Effective_HTMs",
        variables: ["HTM_TPH_all_HTMs", "Effective_HTMs"]
    },
    vtmTor: {
        name: "Effective VTMs / PPS (VTM TOR)",
        formula: "Effective_VTMs / Number_of_PPS",
        variables: ["Effective_VTMs", "Number_of_PPS"]
    },
    htmTor: {
        name: "Effective HTMs / PPS (HTM TOR)",
        formula: "Effective_HTMs / Number_of_PPS",
        variables: ["Effective_HTMs", "Number_of_PPS"]
    },
    idealVtmTor: {
        name: "Ideal VTM TOR",
        formula: "(Cycle_time + OWT) / Target_TPH",
        variables: ["Cycle_time", "OWT", "Target_TPH"]
    },
    idealHtmTor: {
        name: "Ideal HTM TOR",
        formula: "(Cycle_time + OWT) / Target_TPH",
        variables: ["Cycle_time", "OWT", "Target_TPH"]
    }
};

// ========================================
// STUB FUNCTIONS (NOT EXECUTED)
// ========================================

/**
 * Placeholder function - does NOT call DB
 * @param {string} kpiId - The KPI identifier
 * @returns {null} - Returns null until DB connection is configured
 */
function fetchKPIData(kpiId) {
    const query = KPI_QUERIES[kpiId];
    if (!query) {
        console.warn(`[STUB] KPI not found: ${kpiId}`);
        return null;
    }
    
    console.log(`[STUB] Would fetch from ${query.database}.${query.table}:`);
    console.log(`[STUB] Query: ${query.query}`);
    console.log(`[STUB] Columns: ${query.columns.join(', ')}`);
    
    // Return null until DB path is configured
    return null;
}

/**
 * Get all available queries
 * @returns {Object} - All KPI query definitions
 */
function getAllQueries() {
    return KPI_QUERIES;
}

/**
 * Get all DB NOT FOUND KPIs
 * @returns {Array} - List of KPIs without DB mapping
 */
function getDbNotFoundKpis() {
    return DB_NOT_FOUND_KPIS;
}

/**
 * Get all formulas
 * @returns {Object} - All mathematical formula definitions
 */
function getFormulas() {
    return KPI_FORMULAS;
}

/**
 * Get query by ID
 * @param {string} kpiId - The KPI identifier
 * @returns {Object|null} - Query definition or null
 */
function getQueryById(kpiId) {
    return KPI_QUERIES[kpiId] || null;
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { 
        KPI_QUERIES, 
        DB_NOT_FOUND_KPIS, 
        KPI_FORMULAS,
        fetchKPIData,
        getAllQueries,
        getDbNotFoundKpis,
        getFormulas,
        getQueryById
    };
}
