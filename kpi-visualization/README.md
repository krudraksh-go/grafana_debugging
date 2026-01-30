# UPH Architecture Tree Visualization

Interactive visualization of the UPH (Units Per Hour) KPI architecture, mapping KPIs to their underlying database tables and source files.

## Overview

This visualization is based on the **UPH - Xmind AI.pdf** architecture document and maps each KPI to:
- Actual InfluxDB measurements (root tables)
- Derived tables (Airflow DAGs)
- Source Python files
- Formulas and calculations

## UPH Tree Structure

The UPH architecture is broken down into the following major components:

### 1. Picks per tote
- **Orderlines per tote** → `orderline_transactions` (src/orderline_transactions.py)
- **Quantity per orderline** → `system_order_stats` (src/system_order_stats_only_for_cloud_sites.py)

### 2. Ideal / System push
- **MVTS** → MSU movements (DB not found)
- **TPH all PPS** → `ppstask_events` (GreyOrange)
- **Operator pull TPH** → Flow transactions
- **OWT per unit** → `flow_transactions_sec_alteryx` (src/Flow_transactions_sec_alteryx.py)
- **PPF** → `picks_per_rack_face` (src/pick_per_rack_face.py)
- **# of logged in PPS** → `pps_data` (GreyOrange)

### 3. VTM TPH all VTMs (Vertical Transfer Machines)
- **Effective VTMs** → `ranger_events` (GreyOrange)
- **GMC** → Grey Motion Control (DB not found)
- **Cycle time** → `relay_bot_journey` (src/relay_bot_journey.py)
- **VTM aisle changes** → `vtm_aisle_change` (src/vtm_aisle_change.py)
- **Nav efficiency** → `task_cycle_times_summary` (src/consolidated_task_cycle_times.py)
- **Relay utilization** → `relay_utilization` (src/relay_utilization.py)

### 4. HTM TPH all HTMs (Horizontal Transfer Machines)
- **Effective HTMs** → `ranger_events` (GreyOrange, ranger_type='htm')
- **Cycle time** → `relay_bot_journey` (src/relay_bot_journey.py)
- Journey breakdown: Pick from relay, Towards PPS, Picking, Back to store

### 5. Technical metrics
- **Network latency** (GMC ↔ VDA ↔ Firmware) → DB not found

### 6. Put / back to store
- **item_put** → GreyOrange
- **put_uph** → (src/put_uph.py)

### 7. UPH 30min (Dashboard)
- **interval_throughput** → (src/pick_uph.py)
- **item_picked** → GreyOrange (root table)
- **flow_events** → GreyOrange (root table)

## Database Mapping Summary

### Root Tables (GreyOrange InfluxDB)
| Table | Description | Used By |
|-------|-------------|---------|
| `item_picked` | Real-time pick events | UPH, PPF, Cycle time |
| `item_put` | Real-time put events | Put UPH |
| `flow_events` | Operator interaction events | OWT, Flow transactions |
| `ppstask_events` | Robot task events | TPH, OWT |
| `pps_data` | PPS status snapshots | Logged in PPS |
| `ranger_events` | VTM/HTM events | Bot journey, VTM count |
| `task_cycle_times` | Robot cycle metrics | Navigation efficiency |

### Derived Tables (Airflow/Alteryx)
| Table | DAG | Schedule | Source File |
|-------|-----|----------|-------------|
| `interval_throughput` | Interval_throughput | */15 * * * * | src/pick_uph.py |
| `flow_transactions_sec_alteryx` | Flow_transactions | 25 * * * * | src/Flow_transactions_sec_alteryx.py |
| `picks_per_rack_face` | Picks_per_rack_face | */5 * * * * | src/pick_per_rack_face.py |
| `orderline_transactions` | Orderline_transactions | 35 * * * * | src/orderline_transactions.py |
| `relay_bot_journey` | Relay_Bot_Journey | */15 * * * * | src/relay_bot_journey.py |
| `relay_utilization` | relay_utilization | 15 * * * * | src/relay_utilization.py |
| `vtm_aisle_change` | vtm_aisle_change | */15 * * * * | src/vtm_aisle_change.py |
| `task_cycle_times_summary` | task_cycle_times_summary | 15 * * * * | src/consolidated_task_cycle_times.py |

## KPIs with Unknown DB Sources

The following KPIs from the PDF could not be mapped to database tables:

- **GMC** (Grey Motion Control) - Internal service, not in InfluxDB
- **MVTS** (MSU movements) - Inferred from ppstask_events
- **Work created/assigned** - GMC internal metrics
- **Blocked due to bin promotion** - GMC internal
- **Bot error counts** - Not found in current DAGs
- **Network latency** (GMC ↔ VDA ↔ Firmware) - Not logged to InfluxDB
- **Nav factor on highways/racking** - Not calculated separately
- **Hopping %** (PPS to PPS) - Not found
- **Waiting in queue** - Not found

## Running the Visualization

1. Start the Python server:
```bash
cd kpi-visualization
python serve.py
```

2. Open browser at: http://localhost:8000

## Node Types Legend

| Icon | Type | Color | Description |
|------|------|-------|-------------|
| 📈 | KPI | Blue | Key Performance Indicator |
| ⚙️ | Derived | Purple | Derived/Calculated table from DAG |
| 💾 | Root | Green | Root InfluxDB measurement |
| 📋 | Column | Orange | Table column |
| 🔢 | Calculation | Pink | Formula/calculation node |
| 🔗 | External | Cyan | External data source |

## Verification Status

- **✓ Verified**: Mapped to actual source code and database
- **⚠ Inferred**: Inferred from context, not directly verified
- **note: "DB not found"**: Could not find database mapping

## Source Files Reference

```
src/
├── pick_uph.py                    # UPH calculation (interval_throughput)
├── Flow_transactions_sec_alteryx.py # OWT/cycle time breakdown
├── pick_per_rack_face.py          # PPF calculation
├── orderline_transactions.py       # Orderline metrics
├── relay_bot_journey.py           # VTM/HTM journey metrics
├── relay_utilization.py           # Relay position tracking
├── vtm_aisle_change.py            # VTM aisle changes
├── consolidated_task_cycle_times.py # Navigation efficiency
├── put_uph.py                     # Put operations UPH
└── system_order_stats_only_for_cloud_sites.py # Order stats
```
