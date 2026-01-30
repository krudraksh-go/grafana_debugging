# KPI Architecture Tree Skill

This skill helps you build structural dependency trees for KPIs by analyzing Grafana dashboards and tracing calculations through your codebase.

## Quick Start

Simply ask questions like:

- "Build an architecture tree for **Site Uptime Percentage**"
- "Map the dependencies for **Battery Efficiency**"
- "Show me how **Total Energy Output** is calculated"
- "Trace the data sources for **System Health Score**"

The agent will automatically:
1. Search your codebase for KPI references
2. Trace calculations and queries
3. Infer database tables and columns
4. Build a hierarchical dependency tree

## What You'll Get

Three outputs for each KPI:

1. **ASCII Tree** - Human-readable visualization showing the full hierarchy
2. **Structured JSON** - Machine-readable dependency graph
3. **Inferred Entities** - List of all database tables, columns, and relationships discovered

## Example Output

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

## What This Skill Does

- ✅ Maps KPI calculation logic and dependencies
- ✅ Traces data flow from KPI to database entities
- ✅ Infers database schema from code
- ✅ Documents source locations for every component
- ✅ Handles complex multi-source KPIs

## What This Skill Doesn't Do

- ❌ Debug KPI calculations or find bugs
- ❌ Analyze data anomalies or quality issues
- ❌ Suggest new KPIs to track
- ❌ Access actual databases (all schema is inferred)

## Files

- **SKILL.md** - Main workflow and instructions
- **examples.md** - Sample architecture trees for various KPI types
- **reference.md** - Detailed output format specifications and inference rules

## Project Context

This skill is tailored for analyzing KPIs in systems involving:
- Grafana dashboards for monitoring
- Python codebases with SQL queries and data pipelines
- Energy/battery management systems
- Site operational metrics

All database entities are inferred from code since direct database access is not available.
