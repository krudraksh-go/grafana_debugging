# KPI Database Mapping Documentation

## Overview

This document provides a comprehensive mapping of all KPIs from `UPH.yaml` to their corresponding database tables, columns, queries, and mathematical formulas. It also identifies KPIs that have **no database mapping** (marked as DB NOT FOUND).

**Important**: All queries are stored for reference but are **NOT EXECUTED**. Database connection path to be configured later.

---

## Table of Contents

1. [Summary Statistics](#summary-statistics)
2. [KPIs WITH Database Mapping](#kpis-with-database-mapping)
3. [KPIs WITHOUT Database Mapping (DB NOT FOUND)](#kpis-without-database-mapping-db-not-found)
4. [Database Tables Reference](#database-tables-reference)
5. [Mathematical Formulas](#mathematical-formulas)
6. [Source File References](#source-file-references)

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Total KPI Nodes | 85+ |
| KPIs with DB Mapping | 28 |
| KPIs without DB Mapping (DB NOT FOUND) | 39 |
| Unique Database Tables | 8 |
| InfluxDB Databases | GreyOrange, Alteryx, airflow |
| PostgreSQL Databases | platform_srms |

---

## KPIs WITH Database Mapping

### 1. Picks per tote Section

#### Orderlines per tote
- **Database**: `GreyOrange.item_picked`
- **Columns**: `num_of_orderlines`, `tote_id`
- **Query**:
```sql
SELECT mean(num_of_orderlines) 
FROM "GreyOrange"."autogen"."item_picked" 
WHERE installation_id =~ /^$InstallationId$/ 
AND time > $start AND time <= $end
```
- **Source File**: `src/orderline_transactions.py`

#### Quantity per orderline
- **Database**: `platform_srms.stock_unit` (PostgreSQL)
- **Columns**: `quantity`, `station_type`, `storage_type`
- **Query**:
```sql
SELECT station_type, storage_type, avg(stock_unit.quantity) 
FROM service_request 
JOIN service_request_children ON service_request.id = service_request_children.service_request_id 
JOIN service_request_expectations ON service_request_children.servicerequests_id = service_request_expectations.service_request_id 
JOIN container_stock_units ON service_request_expectations.expectations_id = container_stock_units.container_id 
JOIN stock_unit ON container_stock_units.stockunits_id = stock_unit.id 
WHERE service_request.status IN ('CREATED') 
GROUP BY station_type, storage_type
```
- **Source File**: `src/system_order_stats_only_for_cloud_sites.py`

---

### 2. TPH all PPS Section

#### TPH all PPS
- **Database**: `airflow.interval_throughput`
- **Columns**: `UPH_60min`, `pps_id`, `station_type`, `storage_type`
- **Query**:
```sql
SELECT mean(UPH_60min) 
FROM "airflow"."autogen"."interval_throughput" 
WHERE installation_id =~ /^$InstallationId$/ 
AND time > $start AND time <= $end
```
- **Source File**: `src/pick_uph.py`

---

### 3. Operator pull TPH Section

#### OWT per unit
- **Database**: `GreyOrange.ppstask_events`
- **Columns**: `value`, `event`, `task_type`, `pps_id`
- **Query**:
```sql
SELECT mean(value)/1000 
FROM "GreyOrange"."autogen"."ppstask_events" 
WHERE installation_id =~ /^$InstallationId$/ 
AND event='rack_started_to_depart_pps' 
AND task_type='pick' 
AND value<550000 
AND pps_id =~ /^$PpsId$/ 
AND time > $start AND time <= $end
```
- **Formula**: `mean(value) / 1000` (convert ms to seconds)
- **Source File**: Grafana dashboard

#### PPF (Picks Per Face)
- **Database**: `GreyOrange.picks_per_rack_face`
- **Columns**: `total_picks_value`, `total_picks`, `pps_id`, `rack`, `face`
- **Query**:
```sql
SELECT mean(total_picks_value) 
FROM "GreyOrange"."autogen"."picks_per_rack_face" 
WHERE installation_id =~ /^$InstallationId$/ 
AND pps_id =~ /^$PpsId$/ 
AND time > $start AND time <= $end
```
- **Source File**: `src/pick_per_rack_face.py`

#### # of logged in PPS
- **Database**: `GreyOrange.pps_data`
- **Columns**: `value`, `front_logged_in`, `mode`, `pps_id`
- **Query**:
```sql
SELECT count(value)*5 
FROM "GreyOrange"."autogen"."pps_data" 
WHERE installation_id =~ /^$InstallationId$/ 
AND front_logged_in='true' 
AND mode='pick' 
AND pps_id =~ /^$PpsId$/ 
AND time > $start AND time <= $end 
GROUP BY time($TimeInterval)
```
- **Note**: Data recorded every 5 mins, hence *5
- **Source File**: Grafana dashboard

---

### 4. VTM TPH Section

#### VTM TPH all VTMs
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `ranger_type`, `event`
- **Query**:
```sql
SELECT count(*) / (time_range_hours) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE ranger_type='vtm' 
AND event='cycle_time' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### # of working VTMs
- **Database**: `GreyOrange.ranger_events`
- **Columns**: `ranger_id`, `ranger_type`, `event`, `task_status`
- **Query**:
```sql
SELECT count(distinct ranger_id) 
FROM "GreyOrange"."autogen"."ranger_events" 
WHERE ranger_type='vtm' 
AND event='task_completed' 
AND time > $start AND time <= $end
```

#### Blocked due to relay position unavailability
- **Database**: `Alteryx.relay_utilization`
- **Columns**: `dwell_time`, `state`, `aisle_id`, `current_location`
- **Query**:
```sql
SELECT mean(dwell_time) 
FROM "Alteryx"."autogen"."relay_utilization" 
WHERE state='occupied' 
AND time > $start AND time <= $end 
GROUP BY aisle_id
```
- **Source File**: `src/relay_utilization.py`

#### OWT (Operator Working Time)
- **Database**: `airflow.flow_transactions_sec_alteryx`
- **Columns**: `total_time`, `quantity`
- **Query**:
```sql
SELECT mean(total_time) 
FROM "airflow"."autogen"."flow_transactions_sec_alteryx" 
WHERE time > $start AND time <= $end
```
- **Source File**: `src/Flow_transactions_sec_alteryx.py`

#### Cycle time (VTM)
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`, `ranger_type`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='cycle_time' 
AND ranger_type='vtm' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Number of PPS
- **Database**: `GreyOrange.pps_data`
- **Columns**: `pps_id`, `mode`
- **Query**:
```sql
SELECT count(distinct pps_id) 
FROM "GreyOrange"."autogen"."pps_data" 
WHERE mode='pick' 
AND time > $start AND time <= $end
```

#### Horizontal movement
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT sum(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event IN ('Time to Reach IO Point', 'Time to Reach PPS') 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### VTM nav factor
- **Database**: `Alteryx.task_cycle_times_summary`
- **Columns**: `nav_factor`, `ranger_type`
- **Query**:
```sql
SELECT mean(nav_factor) 
FROM "Alteryx"."autogen"."task_cycle_times_summary" 
WHERE ranger_type='vtm' 
AND time > $start AND time <= $end
```
- **Source File**: `src/consolidated_task_cycle_times.py`

#### % aisle changes
- **Database**: `Alteryx.vtm_aisle_change`
- **Columns**: `aisle_id`, `prev_aisle_id`
- **Query**:
```sql
SELECT count(aisle_change)/count(*)*100 
FROM "Alteryx"."autogen"."vtm_aisle_change" 
WHERE time > $start AND time <= $end
```
- **Source File**: `src/vtm_aisle_change.py`

#### Fetch / drop
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`, `action_type`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event IN ('Time to Lift a tote', 'Time to drop tote') 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### % n-deep digging
- **Database**: `Alteryx.relay_journey_raw`
- **Columns**: `current_location_depth`
- **Query**:
```sql
SELECT mean(current_location_depth) 
FROM "Alteryx"."autogen"."relay_journey_raw" 
WHERE current_location_depth > 0 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Storable to relay TPH
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT count(*) / (time_range_hours) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='storable_to_relay_point' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Relay to storable TPH
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT count(*) / (time_range_hours) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='relay_point_to_station' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

---

### 5. Avg. relay position occupancy Section

#### Avg. relay position occupancy
- **Database**: `Alteryx.relay_utilization`
- **Columns**: `dwell_time`, `state`, `aisle_id`, `current_location`
- **Query**:
```sql
SELECT mean(dwell_time) 
FROM "Alteryx"."autogen"."relay_utilization" 
WHERE state='occupied' 
AND time > $start AND time <= $end 
GROUP BY aisle_id
```
- **Source File**: `src/relay_utilization.py`

---

### 6. HTM TPH Section

#### HTM TPH all HTMs
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `ranger_type`, `event`
- **Query**:
```sql
SELECT count(*) / (time_range_hours) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE ranger_type='htm' 
AND event='cycle_time' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### # of working HTMs
- **Database**: `GreyOrange.ranger_events`
- **Columns**: `ranger_id`, `ranger_type`, `event`
- **Query**:
```sql
SELECT count(distinct ranger_id) 
FROM "GreyOrange"."autogen"."ranger_events" 
WHERE ranger_type='htm' 
AND event='task_completed' 
AND time > $start AND time <= $end
```

#### Cycle time (HTM)
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`, `ranger_type`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='cycle_time' 
AND ranger_type='htm' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Nav factor (HTM)
- **Database**: `Alteryx.task_cycle_times_summary`
- **Columns**: `nav_factor`, `ranger_type`
- **Query**:
```sql
SELECT mean(nav_factor) 
FROM "Alteryx"."autogen"."task_cycle_times_summary" 
WHERE ranger_type='htm' 
AND time > $start AND time <= $end
```
- **Source File**: `src/consolidated_task_cycle_times.py`

#### Pick from relay
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='tote_loaded' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Towards PPS
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='reached_pps' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Picking
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='going_pps_exit' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Back to store
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='station_to_relay_point' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

#### Drop on relay
- **Database**: `Alteryx.relay_bot_journey`
- **Columns**: `time_diff`, `event`
- **Query**:
```sql
SELECT mean(time_diff) 
FROM "Alteryx"."autogen"."relay_bot_journey" 
WHERE event='tote_unloaded' 
AND time > $start AND time <= $end
```
- **Source File**: `src/relay_bot_journey.py`

---

### 7. Put / back to store inputs

- **Database**: `GreyOrange.item_put`
- **Columns**: `uom_quantity_int`, `pps_id`, `station_type`, `storage_type`
- **Query**:
```sql
SELECT sum(uom_quantity_int) 
FROM "GreyOrange"."autogen"."item_put" 
WHERE installation_id =~ /^$InstallationId$/ 
AND time > $start AND time <= $end
```
- **Source File**: `src/put_uph.py`

---

## KPIs WITHOUT Database Mapping (DB NOT FOUND)

The following KPIs from `UPH.yaml` have **NO database mapping**. These are either:
- GMC internal calculations
- Real-time state values
- Configuration values
- Statistical calculations not persisted
- Network metrics not centrally logged

### VTM Section (12 KPIs)

| KPI Name | Reason | Parent Node |
|----------|--------|-------------|
| Operator pull TPH | Derived metric - no direct DB table | TPH all PPS |
| Effective VTMs | GMC internal calculation | VTM TPH all VTMs |
| Work created | GMC internal state | Effective VTMs > Breakup #1 |
| Distribution of tasks | GMC internal | Work created |
| Created task count | GMC internal | Work created |
| Work assigned | GMC internal state | Breakup #1 |
| Bot error | No centralized error table | Work assigned |
| # of healthy VTMs (non-error) | Inverse of error count, not tracked | Breakup #1 |
| L1 VTM TOR | Configuration value, not from DB | Breakup #2 |
| Distance | Derived from coordinates, not stored | Horizontal movement |
| Vertical movement | Not tracked separately | Cycle time |
| Mean vertical rack level traversed | Not stored in DB | Vertical movement |
| % telescopic movement | Not tracked | Vertical movement |

### Relay Section (4 KPIs)

| KPI Name | Reason | Parent Node |
|----------|--------|-------------|
| Has tasks & towards PPS | Real-time GMC state | Avg relay position occupancy |
| Doesn't have any task and eligible for back to store | Real-time GMC state | Avg relay position occupancy |
| Variance of relay position occupancy | Statistical calculation | Avg relay position occupancy |
| Work created variance | Statistical calculation | Variance of relay position occupancy |

### HTM Section (17 KPIs)

| KPI Name | Reason | Parent Node |
|----------|--------|-------------|
| Effective HTMs | GMC internal calculation | HTM TPH all HTMs |
| Work created (HTM) | GMC internal | Effective HTMs > Breakup #1 |
| Count of 'Has tasks & towards PPS' totes on relay | Real-time GMC state | Work created (HTM) |
| Blocked due to relay position unavailability because of back to store | Real-time GMC state | Work created (HTM) |
| Distribution of 'Has tasks & towards PPS' totes on relay | Real-time GMC state | Work created (HTM) |
| Work assigned (HTM) | GMC internal | Breakup #1 |
| Blocked due to bin promotion | Not tracked | Work assigned (HTM) |
| Bot error (HTM) | No centralized table | Work assigned (HTM) |
| # of healthy HTMs (non-error) | Not tracked | Breakup #1 |
| L1 HTM TOR | Configuration value | Breakup #2 |
| Nav factor on highways | Granular nav not tracked | Nav factor > Breakup #1 |
| Nav factor on racking | Granular nav not tracked | Nav factor > Breakup #1 |
| Dual cycle distance | Not stored | Cycle time > Navigation |
| Current location to source | Real-time calculation | Cycle time > Breakup #2 |
| waiting in queue | Not tracked as separate metric | Cycle time > Breakup #2 |
| PPS to PPS | Hop events not aggregated | Cycle time > Pick |
| Hopping % | Not tracked | PPS to PPS |

### Technical Section (5 KPIs)

| KPI Name | Reason | Parent Node |
|----------|--------|-------------|
| Network latency | No centralized latency database | Technical metrics |
| GMC to VDA | Internal network metric | Network latency |
| VDA to firmware | Internal network metric | Network latency |
| Firmware to VDA | Internal network metric | Network latency |
| VDA to GMC | Internal network metric | Network latency |

---

## Database Tables Reference

### InfluxDB - GreyOrange Database

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `item_picked` | Pick transactions | `uom_quantity_int`, `num_of_orderlines`, `pps_id`, `tote_id` |
| `picks_per_rack_face` | Picks per rack face aggregation | `total_picks_value`, `total_picks`, `rack`, `face` |
| `pps_data` | PPS status and login info | `pps_id`, `mode`, `front_logged_in`, `value` |
| `ppstask_events` | PPS task timing events | `value`, `event`, `task_type`, `pps_id` |
| `ranger_events` | VTM/HTM task events | `ranger_id`, `ranger_type`, `event`, `task_status` |
| `item_put` | Put transactions | `uom_quantity_int`, `pps_id`, `station_type` |

### InfluxDB - Alteryx Database

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `relay_bot_journey` | VTM/HTM journey metrics | `time_diff`, `event`, `ranger_type`, `ranger_id` |
| `relay_journey_raw` | Raw journey data | `current_location_depth`, `event`, `time_diff` |
| `relay_utilization` | Relay position occupancy | `dwell_time`, `state`, `aisle_id`, `current_location` |
| `task_cycle_times_summary` | Cycle time summaries | `nav_factor`, `ranger_type` |
| `vtm_aisle_change` | VTM aisle change tracking | `aisle_id`, `prev_aisle_id` |

### InfluxDB - airflow Database

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `interval_throughput` | TPH calculations | `UPH_60min`, `UPH_30min`, `pps_id` |
| `flow_transactions_sec_alteryx` | OWT metrics | `total_time`, `quantity` |

### PostgreSQL - platform_srms Database

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `service_request` | Service request master | `id`, `status`, `type`, `station_type` |
| `service_request_children` | SR children | `service_request_id`, `servicerequests_id` |
| `service_request_expectations` | SR expectations | `service_request_id`, `expectations_id` |
| `container_stock_units` | Container to stock unit mapping | `container_id`, `stockunits_id` |
| `stock_unit` | Stock unit details | `id`, `quantity` |

---

## Mathematical Formulas

### Primary UPH Formula

```
UPH = (3600 × PPF) / (R2R_time + PPF × (total_OWT / items_picked))

Where:
- PPF = Picks Per Face (mean(total_picks_value) from picks_per_rack_face)
- R2R_time = Rack-to-Rack time (seconds)
- total_OWT = Total Operator Working Time (seconds)
- items_picked = Total items picked
```

### Derived Formulas

| Formula Name | Expression | Variables |
|--------------|------------|-----------|
| Picks per tote | `sum(uom_quantity_int) / count(distinct tote_id)` | uom_quantity_int, tote_id |
| TPH / VTM | `VTM_TPH_all_VTMs / Effective_VTMs` | VTM TPH, Effective VTMs |
| TPH / HTM | `HTM_TPH_all_HTMs / Effective_HTMs` | HTM TPH, Effective HTMs |
| VTM TOR | `Effective_VTMs / Number_of_PPS` | Effective VTMs, PPS count |
| HTM TOR | `Effective_HTMs / Number_of_PPS` | Effective HTMs, PPS count |
| Ideal VTM TOR | `(Cycle_time + OWT) / Target_TPH` | Cycle time, OWT, Target |
| Ideal HTM TOR | `(Cycle_time + OWT) / Target_TPH` | Cycle time, OWT, Target |

---

## Source File References

| Source File | KPIs Derived |
|-------------|--------------|
| `src/relay_bot_journey.py` | VTM TPH, HTM TPH, Cycle time, Horizontal movement, Fetch/drop, Pick from relay, Towards PPS, Picking, Back to store, Drop on relay, Storable to relay TPH, Relay to storable TPH |
| `src/relay_utilization.py` | Avg relay position occupancy, Blocked due to relay position unavailability |
| `src/pick_uph.py` | TPH all PPS (interval_throughput) |
| `src/pick_per_rack_face.py` | PPF |
| `src/orderline_transactions.py` | Orderlines per tote |
| `src/system_order_stats_only_for_cloud_sites.py` | Quantity per orderline |
| `src/Flow_transactions_sec_alteryx.py` | OWT |
| `src/consolidated_task_cycle_times.py` | VTM nav factor, HTM nav factor |
| `src/vtm_aisle_change.py` | % aisle changes |
| `src/put_uph.py` | Put / back to store inputs |
| Grafana dashboard | OWT per unit, # of logged in PPS |

---

## Revision History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-30 | 1.0 | Initial comprehensive mapping from UPH.yaml |

---

*This document is auto-generated from the KPI visualization codebase. All queries are stored for reference but are NOT EXECUTED until database connection is configured.*
