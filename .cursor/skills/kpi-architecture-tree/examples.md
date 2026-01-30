# KPI Architecture Tree Examples

## Example 1: Simple Base KPI

**KPI**: Total Site Count

**Source**: Grafana dashboard panel showing site count

**Tree**:
```
Total Site Count
└─ QUERY: SELECT COUNT(DISTINCT site_id) FROM sites
   └─ [INFERRED] sites.site_id
      Source: dashboard-1769590401575.json:panel_12
      Type: NUMERIC/ID
```

**Structured Output**:
```json
{
  "kpi": "Total Site Count",
  "type": "base",
  "dependencies": [
    {
      "type": "query",
      "query": "SELECT COUNT(DISTINCT site_id) FROM sites",
      "source": "dashboard-1769590401575.json:panel_12",
      "tables": [
        {
          "name": "sites",
          "columns": ["site_id"],
          "inferred": true
        }
      ]
    }
  ]
}
```

---

## Example 2: Derived KPI with Multiple Dependencies

**KPI**: Battery Efficiency Percentage

**Source**: `src/battery_kpi.py:calculate_efficiency()`

**Tree**:
```
Battery Efficiency Percentage
├─ CALCULATION: (actual_output / theoretical_capacity) * 100
│  Source: src/battery_kpi.py:78
│  ├─ INTERMEDIATE: actual_output
│  │  └─ AGGREGATION: AVG(energy_delivered)
│  │     └─ [INFERRED] battery_metrics.energy_delivered
│  │        Source: src/battery_kpi.py:45
│  │        Type: NUMERIC (kWh assumed)
│  └─ INTERMEDIATE: theoretical_capacity
│     └─ QUERY: SELECT rated_capacity FROM battery_specs
│        ├─ [INFERRED] battery_specs.rated_capacity
│        │  Source: src/battery_kpi.py:52
│        │  Type: NUMERIC
│        └─ JOIN: battery_metrics.battery_id = battery_specs.id
│           Source: src/battery_kpi.py:54
└─ FILTER: timestamp BETWEEN start_date AND end_date
   Source: src/battery_kpi.py:60
```

**Inferred Entities**:
```
TABLES:
  - battery_metrics (inferred from: src/battery_kpi.py:45)
  - battery_specs (inferred from: src/battery_kpi.py:52)

COLUMNS:
  - battery_metrics.energy_delivered [NUMERIC] (AVG aggregation)
  - battery_metrics.battery_id [ID] (JOIN condition)
  - battery_metrics.timestamp [DATETIME] (date filtering)
  - battery_specs.id [ID] (JOIN condition)
  - battery_specs.rated_capacity [NUMERIC] (SELECT field)

RELATIONSHIPS:
  - battery_metrics.battery_id → battery_specs.id (FOREIGN KEY, inferred)
```

---

## Example 3: Complex Multi-Source KPI

**KPI**: Site Operational Score

**Source**: Multiple files in `src/`

**Tree**:
```
Site Operational Score
├─ CALCULATION: weighted_average(uptime_score, performance_score, health_score)
│  Source: src/all_site_kpi.py:235
│  ├─ COMPONENT: uptime_score (weight: 0.4)
│  │  └─ CALCULATION: (operational_hours / total_hours) * 100
│  │     Source: src/uptime_kpi.py:89
│  │     ├─ [INFERRED] site_status.operational_hours
│  │     │  Source: src/uptime_kpi.py:92
│  │     └─ [INFERRED] site_status.total_hours
│  │        Source: src/uptime_kpi.py:93
│  ├─ COMPONENT: performance_score (weight: 0.35)
│  │  └─ DERIVED KPI: Energy Output Efficiency
│  │     Source: src/performance_kpi.py:156
│  │     └─ [See separate tree for Energy Output Efficiency]
│  └─ COMPONENT: health_score (weight: 0.25)
│     └─ CALCULATION: AVG(component_health_values)
│        Source: src/health_kpi.py:67
│        └─ SUBQUERY: SELECT health_value FROM component_health
│           ├─ [INFERRED] component_health.health_value
│           │  Source: src/health_kpi.py:70
│           └─ JOIN: site_components.id = component_health.component_id
│              └─ [INFERRED] site_components.id, component_health.component_id
│                 Source: src/health_kpi.py:73
```

---

## Example 4: KPI with Time-Series Aggregation

**KPI**: Daily Average Cell Voltage

**Source**: `src/cell_voltages_ayx_logic.py`

**Tree**:
```
Daily Average Cell Voltage
├─ CALCULATION: AVG(cell_voltage) GROUP BY DATE(timestamp)
│  Source: src/cell_voltages_ayx_logic.py:145
│  ├─ [INFERRED] cell_readings.cell_voltage
│  │  Source: src/cell_voltages_ayx_logic.py:148
│  │  Type: NUMERIC (Volts, inferred from field name)
│  └─ [INFERRED] cell_readings.timestamp
│     Source: src/cell_voltages_ayx_logic.py:149
│     Type: DATETIME
├─ FILTER: cell_voltage BETWEEN 3.0 AND 4.2
│  Source: src/cell_voltages_ayx_logic.py:152
│  Note: Range check suggests Lithium-ion cell voltage range
└─ JOIN: cells.id = cell_readings.cell_id
   Source: src/cell_voltages_ayx_logic.py:155
   └─ [INFERRED] cells.id, cell_readings.cell_id
```

**Assumptions**:
```
ASSUMPTION: 'cell_voltage' is in Volts (standard unit)
ASSUMPTION: Filter range 3.0-4.2V suggests Lithium-ion chemistry
ASSUMPTION: One reading per cell per timestamp (no duplicates)
```

---

## Example 5: KPI with External Data Source

**KPI**: Weather-Adjusted Energy Output

**Source**: `src/weather_adjustment.py`

**Tree**:
```
Weather-Adjusted Energy Output
├─ CALCULATION: actual_output * adjustment_factor
│  Source: src/weather_adjustment.py:89
│  ├─ [INFERRED] energy_data.actual_output
│  │  Source: src/weather_adjustment.py:92
│  └─ EXTERNAL: adjustment_factor from weather API
│     Source: src/weather_adjustment.py:95
│     API: WeatherService.get_adjustment()
│     NOTE: External dependency - cannot trace further
└─ FILTER: site_id = ?
   Source: src/weather_adjustment.py:98
```

**Limitations**:
```
LIMITATION: 'adjustment_factor' comes from external API
LIMITATION: Cannot trace weather API calculation logic
RECOMMENDATION: Document external API contract separately
```

---

## Example 6: Handling Ambiguous Calculations

**KPI**: System Efficiency

**Source**: Multiple possible definitions found

**Tree**:
```
System Efficiency [AMBIGUOUS - Multiple Definitions Found]

DEFINITION A (src/efficiency_v1.py:45):
├─ CALCULATION: energy_output / energy_input
│  └─ [INFERRED] energy_flow.output, energy_flow.input

DEFINITION B (src/efficiency_v2.py:123):
├─ CALCULATION: (actual_performance / expected_performance) * 100
│  ├─ [INFERRED] performance_metrics.actual_performance
│  └─ [INFERRED] performance_metrics.expected_performance

DEFINITION C (dashboard-1769590401575.json:panel_45):
└─ CALCULATION: operational_hours / total_hours
   └─ [INFERRED] system_status.operational_hours, system_status.total_hours

RECOMMENDATION: Clarify which definition is canonical
```

---

## Usage Tips

1. **When starting**: Begin with the simplest KPI to understand the codebase structure

2. **For complex KPIs**: Break into sub-trees for each component, then combine

3. **When stuck**: Document what you found and what's missing:
   ```
   PARTIAL TREE: Could trace to 'get_battery_data()' call
   MISSING: Implementation of get_battery_data() not found in codebase
   ```

4. **For large codebases**: Focus on one calculation path at a time, use TODO markers:
   ```
   Site Score
   ├─ uptime_component [COMPLETED]
   ├─ performance_component [TODO]
   └─ health_component [TODO]
   ```
