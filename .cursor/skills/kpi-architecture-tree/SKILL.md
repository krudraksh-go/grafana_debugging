---
name: kpi-architecture-tree
description: Build architecture dependency trees for KPIs by analyzing Grafana dashboards and tracing KPI calculations through the codebase down to inferred database entities. Use when the user asks to map KPI dependencies, understand KPI architecture, trace KPI calculations, analyze dashboard structure, or reverse engineer data pipelines.
---

# KPI Architecture Tree

Build structural dependency trees showing how KPIs are calculated, from dashboard definitions down to inferred database entities.

## Core Principles

- **Structure only**: Map dependencies, not bugs or anomalies
- **Inference-based**: Database schema inferred from code, not accessed directly
- **Clear labeling**: Explicitly mark inferred vs confirmed components
- **Source tracking**: Annotate nodes with file locations

## Workflow

### Step 1: Identify the KPI

1. Search for the KPI name in Grafana dashboard JSON (if provided)
2. Search the codebase for KPI references:
   - Python files with KPI calculations
   - SQL queries mentioning the KPI
   - Configuration or mapping files

3. Classify the KPI:
   - **Base KPI**: Direct aggregation from database
   - **Derived KPI**: Calculated from other metrics

### Step 2: Trace Calculations

Starting from the KPI definition, work backwards:

1. **Identify query logic**:
   - SQL SELECT, JOIN, WHERE clauses
   - ORM queries (SQLAlchemy, Django ORM, etc.)
   - DataFrame transformations (pandas, PySpark)

2. **Extract transformations**:
   - Aggregations (SUM, AVG, COUNT, etc.)
   - Filters and conditions
   - Mathematical operations
   - Time-based grouping

3. **Find dependencies**:
   - Intermediate metrics or variables
   - Other KPIs used in calculation
   - Lookup tables or reference data

### Step 3: Infer Database Entities

Since direct database access is unavailable, infer schema from code:

1. **Extract table names**:
   - FROM clauses in SQL
   - Model class names in ORM
   - DataFrame source references

2. **Extract column names**:
   - SELECT fields
   - Model attributes
   - DataFrame column references
   - Filter conditions

3. **Infer relationships**:
   - JOIN conditions
   - Foreign key references in models
   - Merge operations in data pipelines

4. **Mark as inferred**: Label all database entities as `[INFERRED]`

### Step 4: Build the Tree

Construct a hierarchical tree structure:

**Tree structure**:
```
[KPI_NAME] (target metric)
├─ [CALCULATION] transformation or aggregation
│  ├─ [INTERMEDIATE_METRIC] derived value
│  │  └─ [TABLE.column] [INFERRED] leaf database entity
│  └─ [TABLE.column] [INFERRED] leaf database entity
└─ [QUERY] SQL or data pipeline stage
   ├─ [TABLE.column] [INFERRED] leaf database entity
   └─ [JOIN] relationship
      └─ [TABLE.column] [INFERRED] leaf database entity
```

**Node annotations**:
- **Source location**: File path, function name, line number
- **Type**: CALCULATION, QUERY, TABLE, INTERMEDIATE_METRIC
- **Inference status**: Mark all database entities as [INFERRED]

## Output Format

Provide three outputs:

### 1. ASCII Tree

Human-readable tree using box-drawing characters:

```
Site Uptime Percentage
├─ CALCULATION: (total_uptime_hours / total_hours) * 100
│  ├─ AGGREGATION: SUM(uptime_duration)
│  │  └─ [INFERRED] site_status.uptime_duration
│  │     Source: src/site_kpi.py:45
│  └─ AGGREGATION: COUNT(*) * 24
│     └─ [INFERRED] site_status.timestamp
│        Source: src/site_kpi.py:46
└─ FILTER: site_status.status = 'operational'
   Source: src/site_kpi.py:48
```

### 2. Structured Tree Object

JSON representation for programmatic use:

```json
{
  "kpi": "Site Uptime Percentage",
  "type": "derived",
  "dependencies": [
    {
      "type": "calculation",
      "expression": "(total_uptime_hours / total_hours) * 100",
      "source": "src/site_kpi.py:calculate_uptime",
      "inputs": [
        {
          "type": "aggregation",
          "operation": "SUM",
          "field": "uptime_duration",
          "table": "site_status",
          "inferred": true,
          "source": "src/site_kpi.py:45"
        }
      ]
    }
  ]
}
```

### 3. Inferred Entities Summary

List all inferred database components:

```
INFERRED DATABASE ENTITIES:

Tables:
  - site_status (inferred from: src/site_kpi.py:45, src/site_kpi.py:46)
  - battery_metrics (inferred from: src/battery_analysis.py:23)

Columns:
  - site_status.uptime_duration [NUMERIC] (inferred from SUM aggregation)
  - site_status.timestamp [DATETIME] (inferred from date filtering)
  - site_status.status [STRING] (inferred from equality condition)

Relationships:
  - site_status -> sites (inferred from JOIN on site_id)
```

## Best Practices

1. **Start broad, then narrow**: Begin by searching the entire codebase, then focus on relevant files

2. **Follow the data flow**: Trace from KPI → calculation → query → tables

3. **Handle ambiguity**: When multiple interpretations exist, present all options:
   ```
   Site Efficiency
   ├─ POSSIBLE CALCULATION 1: energy_output / energy_input
   │  └─ [details]
   └─ POSSIBLE CALCULATION 2: operational_hours / total_hours
      └─ [details]
   ```

4. **Document assumptions**:
   ```
   ASSUMPTION: 'battery_health' table exists based on query reference
   ASSUMPTION: 'voltage' column is FLOAT based on arithmetic operations
   ```

5. **Note limitations**: Clearly state what could not be traced:
   ```
   NOTE: Could not trace calculation for 'adjusted_capacity'
   NOTE: External API call detected - data source unknown
   ```

## Quick Reference

**Common patterns to look for**:
- SQL: `SELECT`, `FROM`, `JOIN`, `WHERE`, `GROUP BY`, `HAVING`
- Pandas: `.merge()`, `.groupby()`, `.agg()`, `.query()`, `.loc[]`
- SQLAlchemy: `.query()`, `.filter()`, `.join()`, `.group_by()`

**File patterns to search**:
- Python: `*.py` (especially files with 'kpi', 'metric', 'dashboard')
- SQL: `*.sql`
- Config: `*.json`, `*.yaml` (for Grafana dashboards)

**Common KPI calculation patterns**:
- Percentage: `(part / total) * 100`
- Average: `SUM(value) / COUNT(*)`
- Rate: `event_count / time_period`
- Efficiency: `output / input`

## Additional Resources

- For example architecture trees, see [examples.md](examples.md)
- For detailed output schema, see [reference.md](reference.md)
