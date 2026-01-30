// UPH Architecture Tree - Exact mapping from UPH.yaml
// All queries are STORED but NOT EXECUTED - DB path to be configured later
// Each node contains: name, type, database info, query, columns, formula (where applicable)

const KPI_ARCHITECTURE_DATA = {
    name: "UPH",
    type: "kpi",
    description: "Units Per Hour - Top level throughput metric",
    formula: "UPH = (3600 × PPF) / (R2R_time + PPF × (total_OWT / items_picked))",
    children: [
        // ========================================
        // PICKS PER TOTE
        // ========================================
        {
            name: "Picks per tote",
            type: "kpi",
            formula: "sum(uom_quantity_int) / count(distinct tote_id)",
            children: [
                {
                    name: "Orderlines per tote",
                    type: "metric",
                    database: "GreyOrange.item_picked",
                    columns: ["num_of_orderlines", "tote_id"],
                    query: "SELECT mean(num_of_orderlines) FROM \"GreyOrange\".\"autogen\".\"item_picked\" WHERE installation_id =~ /^$InstallationId$/ AND time > $start AND time <= $end",
                    sourceFile: "src/orderline_transactions.py"
                },
                {
                    name: "Quantity per orderline",
                    type: "metric",
                    database: "platform_srms.stock_unit",
                    columns: ["quantity", "station_type", "storage_type"],
                    query: "SELECT station_type, storage_type, avg(stock_unit.quantity) FROM service_request JOIN service_request_children ON service_request.id = service_request_children.service_request_id JOIN service_request_expectations ON service_request_children.servicerequests_id = service_request_expectations.service_request_id JOIN container_stock_units ON service_request_expectations.expectations_id = container_stock_units.container_id JOIN stock_unit ON container_stock_units.stockunits_id = stock_unit.id WHERE service_request.status IN ('CREATED') GROUP BY station_type, storage_type",
                    sourceFile: "src/system_order_stats_only_for_cloud_sites.py"
                }
            ]
        },

        // ========================================
        // TPH ALL PPS
        // ========================================
        {
            name: "TPH all PPS",
            type: "kpi",
            database: "airflow.interval_throughput",
            columns: ["UPH_60min", "pps_id", "station_type", "storage_type"],
            query: "SELECT mean(UPH_60min) FROM \"airflow\".\"autogen\".\"interval_throughput\" WHERE installation_id =~ /^$InstallationId$/ AND time > $start AND time <= $end",
            sourceFile: "src/pick_uph.py",
            children: [
                // ----------------------------------------
                // OPERATOR PULL TPH
                // ----------------------------------------
                {
                    name: "Operator pull TPH",
                    type: "kpi",
                    dbNotFound: true,
                    note: "Derived metric - no direct DB table",
                    children: [
                        {
                            name: "OWT per unit",
                            type: "metric",
                            database: "GreyOrange.ppstask_events",
                            columns: ["value", "event", "task_type", "pps_id"],
                            query: "SELECT mean(value)/1000 FROM \"GreyOrange\".\"autogen\".\"ppstask_events\" WHERE installation_id =~ /^$InstallationId$/ AND event='rack_started_to_depart_pps' AND task_type='pick' AND value<550000 AND pps_id =~ /^$PpsId$/ AND time > $start AND time <= $end",
                            sourceFile: "Grafana dashboard",
                            formula: "mean(value) / 1000 (convert ms to seconds)"
                        },
                        {
                            name: "PPF",
                            type: "metric",
                            database: "GreyOrange.picks_per_rack_face",
                            columns: ["total_picks_value", "total_picks", "pps_id", "rack", "face"],
                            query: "SELECT mean(total_picks_value) FROM \"GreyOrange\".\"autogen\".\"picks_per_rack_face\" WHERE installation_id =~ /^$InstallationId$/ AND pps_id =~ /^$PpsId$/ AND time > $start AND time <= $end",
                            sourceFile: "src/pick_per_rack_face.py"
                        },
                        {
                            name: "# of logged in PPS",
                            type: "metric",
                            database: "GreyOrange.pps_data",
                            columns: ["value", "front_logged_in", "mode", "pps_id"],
                            query: "SELECT count(value)*5 FROM \"GreyOrange\".\"autogen\".\"pps_data\" WHERE installation_id =~ /^$InstallationId$/ AND front_logged_in='true' AND mode='pick' AND pps_id =~ /^$PpsId$/ AND time > $start AND time <= $end GROUP BY time($TimeInterval)",
                            sourceFile: "Grafana dashboard",
                            note: "Data recorded every 5 mins, hence *5"
                        }
                    ]
                },

                // ----------------------------------------
                // VTM TPH ALL VTMs
                // ----------------------------------------
                {
                    name: "VTM TPH all VTMs",
                    type: "kpi",
                    database: "Alteryx.relay_bot_journey",
                    columns: ["time_diff", "ranger_type", "event"],
                    query: "SELECT count(*) / (time_range_hours) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE ranger_type='vtm' AND event='cycle_time' AND time > $start AND time <= $end",
                    sourceFile: "src/relay_bot_journey.py",
                    children: [
                        {
                            name: "Effective VTMs",
                            type: "calculation",
                            dbNotFound: true,
                            note: "GMC internal calculation",
                            children: [
                                {
                                    name: "Breakup #1",
                                    type: "calculation",
                                    children: [
                                        {
                                            name: "# of working VTMs",
                                            type: "metric",
                                            database: "GreyOrange.ranger_events",
                                            columns: ["ranger_id", "ranger_type", "event", "task_status"],
                                            query: "SELECT count(distinct ranger_id) FROM \"GreyOrange\".\"autogen\".\"ranger_events\" WHERE ranger_type='vtm' AND event='task_completed' AND time > $start AND time <= $end",
                                            children: [
                                                {
                                                    name: "Work created",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "GMC internal state",
                                                    children: [
                                                        {
                                                            name: "Distribution of tasks",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "GMC internal"
                                                        },
                                                        {
                                                            name: "Created task count",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "GMC internal"
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "Work assigned",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "GMC internal state",
                                                    children: [
                                                        {
                                                            name: "Blocked due to relay position unavailability",
                                                            type: "metric",
                                                            database: "Alteryx.relay_utilization",
                                                            columns: ["dwell_time", "state", "aisle_id", "current_location"],
                                                            query: "SELECT mean(dwell_time) FROM \"Alteryx\".\"autogen\".\"relay_utilization\" WHERE state='occupied' AND time > $start AND time <= $end GROUP BY aisle_id",
                                                            sourceFile: "src/relay_utilization.py"
                                                        },
                                                        {
                                                            name: "Bot error",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "No centralized error table"
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            name: "# of healthy VTMs (non-error)",
                                            type: "calculation",
                                            dbNotFound: true,
                                            note: "Inverse of error count, not tracked"
                                        }
                                    ]
                                },
                                {
                                    name: "Breakup #2",
                                    type: "calculation",
                                    children: [
                                        {
                                            name: "Effective VTMs / PPS (VTM TOR)",
                                            type: "calculation",
                                            formula: "Effective_VTMs / Number_of_PPS",
                                            children: [
                                                {
                                                    name: "Ideal VTM TOR = (cycle time math with OWT)",
                                                    type: "calculation",
                                                    formula: "(Cycle_time + OWT) / Target_TPH",
                                                    children: [
                                                        {
                                                            name: "OWT",
                                                            type: "metric",
                                                            database: "airflow.flow_transactions_sec_alteryx",
                                                            columns: ["total_time", "quantity"],
                                                            query: "SELECT mean(total_time) FROM \"airflow\".\"autogen\".\"flow_transactions_sec_alteryx\" WHERE time > $start AND time <= $end",
                                                            sourceFile: "src/Flow_transactions_sec_alteryx.py"
                                                        },
                                                        {
                                                            name: "Cycle time",
                                                            type: "metric",
                                                            database: "Alteryx.relay_bot_journey",
                                                            columns: ["time_diff", "event"],
                                                            query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='cycle_time' AND time > $start AND time <= $end",
                                                            sourceFile: "src/relay_bot_journey.py"
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "L1 VTM TOR",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Configuration value, not from DB"
                                                }
                                            ]
                                        },
                                        {
                                            name: "Number of PPS",
                                            type: "metric",
                                            database: "GreyOrange.pps_data",
                                            columns: ["pps_id", "mode"],
                                            query: "SELECT count(distinct pps_id) FROM \"GreyOrange\".\"autogen\".\"pps_data\" WHERE mode='pick' AND time > $start AND time <= $end"
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            name: "TPH / VTM",
                            type: "calculation",
                            formula: "VTM_TPH_all_VTMs / Effective_VTMs",
                            children: [
                                {
                                    name: "Breakup #1",
                                    type: "calculation",
                                    children: [
                                        {
                                            name: "Cycle time",
                                            type: "metric",
                                            database: "Alteryx.relay_bot_journey",
                                            columns: ["time_diff", "event", "ranger_type"],
                                            query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='cycle_time' AND ranger_type='vtm' AND time > $start AND time <= $end",
                                            sourceFile: "src/relay_bot_journey.py",
                                            children: [
                                                {
                                                    name: "Horizontal movement",
                                                    type: "metric",
                                                    database: "Alteryx.relay_bot_journey",
                                                    columns: ["time_diff", "event"],
                                                    query: "SELECT sum(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event IN ('Time to Reach IO Point', 'Time to Reach PPS') AND time > $start AND time <= $end",
                                                    children: [
                                                        {
                                                            name: "VTM nav factor",
                                                            type: "metric",
                                                            database: "Alteryx.task_cycle_times_summary",
                                                            columns: ["nav_factor", "ranger_type"],
                                                            query: "SELECT mean(nav_factor) FROM \"Alteryx\".\"autogen\".\"task_cycle_times_summary\" WHERE ranger_type='vtm' AND time > $start AND time <= $end",
                                                            sourceFile: "src/consolidated_task_cycle_times.py"
                                                        },
                                                        {
                                                            name: "Distance",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "Derived from coordinates, not stored",
                                                            children: [
                                                                {
                                                                    name: "% aisle changes",
                                                                    type: "metric",
                                                                    database: "Alteryx.vtm_aisle_change",
                                                                    columns: ["aisle_id", "prev_aisle_id"],
                                                                    query: "SELECT count(aisle_change)/count(*)*100 FROM \"Alteryx\".\"autogen\".\"vtm_aisle_change\" WHERE time > $start AND time <= $end",
                                                                    sourceFile: "src/vtm_aisle_change.py"
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "Vertical movement",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Not tracked separately",
                                                    children: [
                                                        {
                                                            name: "Mean vertical rack level traversed",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "Not stored in DB",
                                                            children: [
                                                                {
                                                                    name: "% telescopic movement",
                                                                    type: "calculation",
                                                                    dbNotFound: true,
                                                                    note: "Not tracked"
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "Fetch / drop",
                                                    type: "metric",
                                                    database: "Alteryx.relay_bot_journey",
                                                    columns: ["time_diff", "event", "action_type"],
                                                    query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event IN ('Time to Lift a tote', 'Time to drop tote') AND time > $start AND time <= $end",
                                                    sourceFile: "src/relay_bot_journey.py",
                                                    children: [
                                                        {
                                                            name: "% n-deep digging",
                                                            type: "metric",
                                                            database: "Alteryx.relay_journey_raw",
                                                            columns: ["current_location_depth"],
                                                            query: "SELECT mean(current_location_depth) FROM \"Alteryx\".\"autogen\".\"relay_journey_raw\" WHERE current_location_depth > 0 AND time > $start AND time <= $end",
                                                            sourceFile: "src/relay_bot_journey.py"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                },
                                {
                                    name: "Breakup #2",
                                    type: "calculation",
                                    children: [
                                        {
                                            name: "Storable to relay TPH",
                                            type: "metric",
                                            database: "Alteryx.relay_bot_journey",
                                            columns: ["time_diff", "event"],
                                            query: "SELECT count(*) / (time_range_hours) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='storable_to_relay_point' AND time > $start AND time <= $end",
                                            sourceFile: "src/relay_bot_journey.py"
                                        },
                                        {
                                            name: "Relay to storable TPH",
                                            type: "metric",
                                            database: "Alteryx.relay_bot_journey",
                                            columns: ["time_diff", "event"],
                                            query: "SELECT count(*) / (time_range_hours) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='relay_point_to_station' AND time > $start AND time <= $end",
                                            sourceFile: "src/relay_bot_journey.py"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },

                // ----------------------------------------
                // AVG RELAY POSITION OCCUPANCY
                // ----------------------------------------
                {
                    name: "Avg. relay position occupancy",
                    type: "kpi",
                    database: "Alteryx.relay_utilization",
                    columns: ["dwell_time", "state", "aisle_id", "current_location"],
                    query: "SELECT mean(dwell_time) FROM \"Alteryx\".\"autogen\".\"relay_utilization\" WHERE state='occupied' AND time > $start AND time <= $end GROUP BY aisle_id",
                    sourceFile: "src/relay_utilization.py",
                    children: [
                        {
                            name: "Avg relay position occupancy",
                            type: "metric",
                            database: "Alteryx.relay_utilization",
                            columns: ["dwell_time", "state"],
                            query: "SELECT mean(dwell_time) FROM \"Alteryx\".\"autogen\".\"relay_utilization\" WHERE state='occupied' AND time > $start AND time <= $end",
                            children: [
                                {
                                    name: "Has tasks & towards PPS",
                                    type: "calculation",
                                    dbNotFound: true,
                                    note: "Real-time GMC state"
                                },
                                {
                                    name: "Doesn't have any task and eligible for back to store",
                                    type: "calculation",
                                    dbNotFound: true,
                                    note: "Real-time GMC state"
                                }
                            ]
                        },
                        {
                            name: "Variance of relay position occupancy",
                            type: "calculation",
                            dbNotFound: true,
                            note: "Statistical calculation on dwell_time",
                            children: [
                                {
                                    name: "Work created variance",
                                    type: "calculation",
                                    dbNotFound: true,
                                    note: "Statistical calculation"
                                }
                            ]
                        }
                    ]
                },

                // ----------------------------------------
                // HTM TPH ALL HTMs
                // ----------------------------------------
                {
                    name: "HTM TPH all HTMs",
                    type: "kpi",
                    database: "Alteryx.relay_bot_journey",
                    columns: ["time_diff", "ranger_type", "event"],
                    query: "SELECT count(*) / (time_range_hours) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE ranger_type='htm' AND event='cycle_time' AND time > $start AND time <= $end",
                    sourceFile: "src/relay_bot_journey.py",
                    children: [
                        {
                            name: "Effective HTMs",
                            type: "calculation",
                            dbNotFound: true,
                            note: "GMC internal calculation",
                            children: [
                                {
                                    name: "Breakup #1",
                                    type: "calculation",
                                    children: [
                                        {
                                            name: "# of working HTMs",
                                            type: "metric",
                                            database: "GreyOrange.ranger_events",
                                            columns: ["ranger_id", "ranger_type", "event"],
                                            query: "SELECT count(distinct ranger_id) FROM \"GreyOrange\".\"autogen\".\"ranger_events\" WHERE ranger_type='htm' AND event='task_completed' AND time > $start AND time <= $end",
                                            children: [
                                                {
                                                    name: "Work created",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "GMC internal",
                                                    children: [
                                                        {
                                                            name: "Count of 'Has tasks & towards PPS' totes on relay",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "Real-time GMC state",
                                                            children: [
                                                                {
                                                                    name: "Blocked due to relay position unavailability because of back to store",
                                                                    type: "calculation",
                                                                    dbNotFound: true,
                                                                    note: "Real-time GMC state"
                                                                }
                                                            ]
                                                        },
                                                        {
                                                            name: "Distribution of 'Has tasks & towards PPS' totes on relay",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "Real-time GMC state"
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "Work assigned",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "GMC internal",
                                                    children: [
                                                        {
                                                            name: "Blocked due to bin promotion",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "Not tracked"
                                                        },
                                                        {
                                                            name: "Bot error",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "No centralized table"
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            name: "# of healthy HTMs (non-error)",
                                            type: "calculation",
                                            dbNotFound: true,
                                            note: "Not tracked"
                                        }
                                    ]
                                },
                                {
                                    name: "Breakup #2",
                                    type: "calculation",
                                    children: [
                                        {
                                            name: "Effective HTMs / PPS (HTM TOR)",
                                            type: "calculation",
                                            formula: "Effective_HTMs / Number_of_PPS",
                                            children: [
                                                {
                                                    name: "Ideal HTM TOR = (cycle time math with OWT)",
                                                    type: "calculation",
                                                    formula: "(Cycle_time + OWT) / Target_TPH",
                                                    children: [
                                                        {
                                                            name: "OWT",
                                                            type: "metric",
                                                            database: "airflow.flow_transactions_sec_alteryx",
                                                            columns: ["total_time", "quantity"],
                                                            query: "SELECT mean(total_time) FROM \"airflow\".\"autogen\".\"flow_transactions_sec_alteryx\" WHERE time > $start AND time <= $end",
                                                            sourceFile: "src/Flow_transactions_sec_alteryx.py"
                                                        },
                                                        {
                                                            name: "Cycle time",
                                                            type: "metric",
                                                            database: "Alteryx.relay_bot_journey",
                                                            columns: ["time_diff", "event"],
                                                            query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='cycle_time' AND ranger_type='htm' AND time > $start AND time <= $end",
                                                            sourceFile: "src/relay_bot_journey.py"
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "L1 HTM TOR",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Configuration value"
                                                }
                                            ]
                                        },
                                        {
                                            name: "Number of PPS",
                                            type: "metric",
                                            database: "GreyOrange.pps_data",
                                            columns: ["pps_id", "mode"],
                                            query: "SELECT count(distinct pps_id) FROM \"GreyOrange\".\"autogen\".\"pps_data\" WHERE mode='pick' AND time > $start AND time <= $end"
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            name: "TPH / HTM",
                            type: "calculation",
                            formula: "HTM_TPH_all_HTMs / Effective_HTMs",
                            children: [
                                {
                                    name: "Cycle time",
                                    type: "metric",
                                    database: "Alteryx.relay_bot_journey",
                                    columns: ["time_diff", "event", "ranger_type"],
                                    query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='cycle_time' AND ranger_type='htm' AND time > $start AND time <= $end",
                                    sourceFile: "src/relay_bot_journey.py",
                                    children: [
                                        {
                                            name: "Breakup #1",
                                            type: "calculation",
                                            children: [
                                                {
                                                    name: "Nav factor",
                                                    type: "metric",
                                                    database: "Alteryx.task_cycle_times_summary",
                                                    columns: ["nav_factor", "ranger_type"],
                                                    query: "SELECT mean(nav_factor) FROM \"Alteryx\".\"autogen\".\"task_cycle_times_summary\" WHERE ranger_type='htm' AND time > $start AND time <= $end",
                                                    sourceFile: "src/consolidated_task_cycle_times.py",
                                                    children: [
                                                        {
                                                            name: "Breakup #1",
                                                            type: "calculation",
                                                            children: [
                                                                {
                                                                    name: "Nav factor on highways",
                                                                    type: "calculation",
                                                                    dbNotFound: true,
                                                                    note: "Granular nav not tracked"
                                                                },
                                                                {
                                                                    name: "Nav factor on racking",
                                                                    type: "calculation",
                                                                    dbNotFound: true,
                                                                    note: "Granular nav not tracked"
                                                                },
                                                                {
                                                                    name: "....",
                                                                    type: "calculation",
                                                                    dbNotFound: true,
                                                                    note: "Additional nav factors"
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "Dual cycle distance",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Not stored"
                                                }
                                            ]
                                        },
                                        {
                                            name: "Breakup #2",
                                            type: "calculation",
                                            children: [
                                                {
                                                    name: "Current location to source",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Real-time calculation"
                                                },
                                                {
                                                    name: "Pick from relay",
                                                    type: "metric",
                                                    database: "Alteryx.relay_bot_journey",
                                                    columns: ["time_diff", "event"],
                                                    query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='tote_loaded' AND time > $start AND time <= $end",
                                                    sourceFile: "src/relay_bot_journey.py"
                                                },
                                                {
                                                    name: "Towards PPS",
                                                    type: "metric",
                                                    database: "Alteryx.relay_bot_journey",
                                                    columns: ["time_diff", "event"],
                                                    query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='reached_pps' AND time > $start AND time <= $end",
                                                    sourceFile: "src/relay_bot_journey.py"
                                                },
                                                {
                                                    name: "waiting in queue",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Not tracked as separate metric"
                                                },
                                                {
                                                    name: "Picking",
                                                    type: "metric",
                                                    database: "Alteryx.relay_bot_journey",
                                                    columns: ["time_diff", "event"],
                                                    query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='going_pps_exit' AND time > $start AND time <= $end",
                                                    sourceFile: "src/relay_bot_journey.py"
                                                },
                                                {
                                                    name: "PPS to PPS",
                                                    type: "calculation",
                                                    dbNotFound: true,
                                                    note: "Hop events not aggregated",
                                                    children: [
                                                        {
                                                            name: "Hopping %",
                                                            type: "calculation",
                                                            dbNotFound: true,
                                                            note: "Not tracked"
                                                        }
                                                    ]
                                                },
                                                {
                                                    name: "Back to store",
                                                    type: "metric",
                                                    database: "Alteryx.relay_bot_journey",
                                                    columns: ["time_diff", "event"],
                                                    query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='station_to_relay_point' AND time > $start AND time <= $end",
                                                    sourceFile: "src/relay_bot_journey.py",
                                                    children: [
                                                        {
                                                            name: "Drop on relay",
                                                            type: "metric",
                                                            database: "Alteryx.relay_bot_journey",
                                                            columns: ["time_diff", "event"],
                                                            query: "SELECT mean(time_diff) FROM \"Alteryx\".\"autogen\".\"relay_bot_journey\" WHERE event='tote_unloaded' AND time > $start AND time <= $end",
                                                            sourceFile: "src/relay_bot_journey.py"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },

        // ========================================
        // TECHNICAL METRICS
        // ========================================
        {
            name: "Technical metrics",
            type: "kpi",
            dbNotFound: true,
            note: "Internal network metrics - no centralized DB",
            children: [
                {
                    name: "Network latency",
                    type: "calculation",
                    dbNotFound: true,
                    note: "No centralized latency database",
                    children: [
                        {
                            name: "GMC to VDA",
                            type: "calculation",
                            dbNotFound: true,
                            note: "Internal network metric"
                        },
                        {
                            name: "VDA to firmware",
                            type: "calculation",
                            dbNotFound: true,
                            note: "Internal network metric"
                        },
                        {
                            name: "Firmware to VDA",
                            type: "calculation",
                            dbNotFound: true,
                            note: "Internal network metric"
                        },
                        {
                            name: "VDA to GMC",
                            type: "calculation",
                            dbNotFound: true,
                            note: "Internal network metric"
                        }
                    ]
                }
            ]
        },

        // ========================================
        // PUT / BACK TO STORE INPUTS
        // ========================================
        {
            name: "Put / back to store inputs",
            type: "kpi",
            database: "GreyOrange.item_put",
            columns: ["uom_quantity_int", "pps_id", "station_type", "storage_type"],
            query: "SELECT sum(uom_quantity_int) FROM \"GreyOrange\".\"autogen\".\"item_put\" WHERE installation_id =~ /^$InstallationId$/ AND time > $start AND time <= $end",
            sourceFile: "src/put_uph.py"
        }
    ]
};

// Node type configurations - Updated for light theme
const NODE_TYPES = {
    kpi: { 
        color: "#2563eb",  // Industrial blue
        icon: "📊", 
        label: "KPI" 
    },
    metric: { 
        color: "#059669",  // Green for DB-backed metrics
        icon: "📈", 
        label: "Metric (DB)" 
    },
    calculation: { 
        color: "#7c3aed",  // Purple for calculations
        icon: "🔢", 
        label: "Calculation" 
    },
    derived: { 
        color: "#8b5cf6", 
        icon: "⚙️", 
        label: "Derived" 
    },
    root: { 
        color: "#059669", 
        icon: "💾", 
        label: "Root Table" 
    }
};

// Export for use in visualization
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { KPI_ARCHITECTURE_DATA, NODE_TYPES };
}
