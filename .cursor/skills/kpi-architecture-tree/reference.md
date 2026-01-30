# KPI Architecture Tree Reference

## Node Types

### KPI Node (Root)
- **Type**: `kpi`
- **Required fields**: `name`, `type` (base/derived)
- **Optional fields**: `description`, `unit`, `source`

### Calculation Node
- **Type**: `calculation`
- **Required fields**: `expression`, `source`
- **Optional fields**: `operation_type` (arithmetic/aggregation/conditional)

### Query Node
- **Type**: `query`
- **Required fields**: `query_text`, `source`
- **Optional fields**: `query_type` (SELECT/INSERT/UPDATE)

### Table Node (Inferred)
- **Type**: `table`
- **Required fields**: `name`, `inferred: true`
- **Optional fields**: `columns[]`, `estimated_rows`

### Column Node (Inferred)
- **Type**: `column`
- **Required fields**: `name`, `table`, `inferred: true`
- **Optional fields**: `data_type`, `usage` (filter/aggregation/join)

### Intermediate Metric Node
- **Type**: `intermediate_metric`
- **Required fields**: `name`, `source`
- **Optional fields**: `description`

### Filter Node
- **Type**: `filter`
- **Required fields**: `condition`, `source`
- **Optional fields**: `filter_type` (WHERE/HAVING)

### Join Node
- **Type**: `join`
- **Required fields**: `left_table`, `right_table`, `condition`
- **Optional fields**: `join_type` (INNER/LEFT/RIGHT/OUTER)

### Aggregation Node
- **Type**: `aggregation`
- **Required fields**: `operation` (SUM/AVG/COUNT/MIN/MAX), `field`
- **Optional fields**: `group_by[]`

### External Node
- **Type**: `external`
- **Required fields**: `source_type` (api/file/service)
- **Optional fields**: `endpoint`, `description`

---

## Full JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "KPI Architecture Tree",
  "type": "object",
  "required": ["kpi", "type", "dependencies"],
  "properties": {
    "kpi": {
      "type": "string",
      "description": "Name of the KPI"
    },
    "type": {
      "type": "string",
      "enum": ["base", "derived"],
      "description": "Whether KPI is directly from DB or calculated"
    },
    "unit": {
      "type": "string",
      "description": "Unit of measurement (%, kWh, count, etc.)"
    },
    "description": {
      "type": "string"
    },
    "source": {
      "type": "object",
      "properties": {
        "file": {"type": "string"},
        "function": {"type": "string"},
        "line": {"type": "integer"}
      }
    },
    "dependencies": {
      "type": "array",
      "items": {"$ref": "#/definitions/node"}
    }
  },
  "definitions": {
    "node": {
      "type": "object",
      "required": ["type"],
      "properties": {
        "type": {
          "type": "string",
          "enum": [
            "calculation",
            "query",
            "table",
            "column",
            "intermediate_metric",
            "filter",
            "join",
            "aggregation",
            "external"
          ]
        },
        "source": {
          "type": "object",
          "properties": {
            "file": {"type": "string"},
            "function": {"type": "string"},
            "line": {"type": "integer"}
          }
        },
        "inferred": {
          "type": "boolean",
          "description": "True if entity is inferred from code, not confirmed"
        },
        "inputs": {
          "type": "array",
          "items": {"$ref": "#/definitions/node"}
        }
      }
    }
  }
}
```

---

## Inference Rules

### Inferring Data Types

| Code Pattern | Inferred Type |
|--------------|---------------|
| `SUM(field)`, `AVG(field)` | NUMERIC |
| `COUNT(*)`, `COUNT(field)` | INTEGER |
| `DATE(field)`, `timestamp > date` | DATETIME/DATE |
| `field = 'string'` | STRING/VARCHAR |
| `field IN (1,2,3)` | INTEGER or ENUM |
| `field BETWEEN 0 AND 1` | NUMERIC (0-1 range) |
| `field * 100`, `field / 100` | NUMERIC (likely percentage) |

### Inferring Relationships

| Code Pattern | Inferred Relationship |
|--------------|----------------------|
| `a.id = b.a_id` | b.a_id is FK to a.id |
| `LEFT JOIN b ON a.id = b.ref_id` | b.ref_id references a.id (optional) |
| `INNER JOIN b ON a.id = b.ref_id` | b.ref_id references a.id (required) |

### Inferring Cardinality

| Code Pattern | Inferred Cardinality |
|--------------|---------------------|
| `DISTINCT site_id` | Many records per site |
| `GROUP BY site_id` | Multiple rows per site |
| `MAX(value) per site` | Multiple measurements per site |

---

## ASCII Tree Formatting

### Box-Drawing Characters

```
├─  Branch connector (not last child)
└─  Branch connector (last child)
│   Vertical line (continuation)
```

### Indentation Rules

- 3 spaces per level
- Align box characters vertically
- Use consistent spacing for multi-line content

### Example with Annotations

```
KPI Name                                    <- Root (no indent)
├─ NODE TYPE: description                  <- Level 1 (3 spaces)
│  ├─ [INFERRED] table.column              <- Level 2 (6 spaces)
│  │  Source: file.py:123                  <- Annotation (9 spaces)
│  │  Type: NUMERIC                        <- Annotation (9 spaces)
│  └─ [INFERRED] table.other_column        <- Level 2 sibling (last)
│     Source: file.py:456                  <- Annotation (12 spaces)
└─ NODE TYPE: other description            <- Level 1 sibling (last)
   └─ child node                           <- Level 2 (6 spaces)
```

---

## Common SQL Pattern Recognition

### Aggregations

```sql
-- Pattern: SUM(field)
-- Infers: field is NUMERIC, summing multiple rows

-- Pattern: COUNT(DISTINCT field)
-- Infers: Multiple records per field value

-- Pattern: AVG(field) GROUP BY category
-- Infers: field is NUMERIC, category groups data
```

### Joins

```sql
-- Pattern: LEFT JOIN table ON condition
-- Infers: Optional relationship (right table may have NULL)

-- Pattern: INNER JOIN table ON condition
-- Infers: Required relationship (both tables must match)
```

### Filters

```sql
-- Pattern: WHERE status = 'active'
-- Infers: status is STRING, enumerated values

-- Pattern: WHERE date >= '2024-01-01'
-- Infers: date is DATE/DATETIME type

-- Pattern: WHERE value BETWEEN 0 AND 100
-- Infers: value is NUMERIC, bounded range
```

### Subqueries

```sql
-- Pattern: SELECT ... FROM (SELECT ... FROM table) AS subq
-- Infers: Intermediate calculation step, create intermediate_metric node
```

---

## Python Pattern Recognition

### Pandas Patterns

```python
# Pattern: df.groupby('site_id')['value'].sum()
# Infers: Multiple rows per site_id, value is numeric

# Pattern: df.merge(other_df, on='id')
# Infers: Relationship between df and other_df via 'id'

# Pattern: df['result'] = df['a'] / df['b']
# Infers: Calculation node, a and b are numeric
```

### SQLAlchemy Patterns

```python
# Pattern: session.query(Model.field).filter(Model.status == 'active')
# Infers: Model table, field column, status column (STRING)

# Pattern: query.join(OtherModel, Model.id == OtherModel.ref_id)
# Infers: Relationship via ref_id foreign key
```

---

## Handling Edge Cases

### Multiple Possible Sources

When a KPI could come from multiple sources:

```json
{
  "kpi": "Site Count",
  "type": "ambiguous",
  "possible_sources": [
    {
      "source": "src/count_v1.py",
      "confidence": "high",
      "last_modified": "2024-10-15"
    },
    {
      "source": "dashboard.json:panel_23",
      "confidence": "medium",
      "note": "Older definition"
    }
  ]
}
```

### Missing Implementations

When a function is called but not found:

```json
{
  "type": "external",
  "source_type": "function",
  "name": "get_external_data()",
  "status": "not_found",
  "called_from": "src/kpi.py:45",
  "note": "Implementation not in codebase"
}
```

### Dynamic Queries

When queries are built dynamically:

```json
{
  "type": "query",
  "query_type": "dynamic",
  "template": "SELECT {fields} FROM {table} WHERE {conditions}",
  "source": "src/query_builder.py:89",
  "note": "Actual query varies by runtime parameters"
}
```

---

## Validation Checklist

Before finalizing a tree, verify:

- [ ] All database entities marked as `[INFERRED]`
- [ ] Every node has a source location
- [ ] Root node is the target KPI
- [ ] Leaf nodes are database columns or external sources
- [ ] No circular dependencies
- [ ] Relationships are logically consistent
- [ ] Data types are reasonable for operations
- [ ] Assumptions are documented
- [ ] Limitations are noted
- [ ] Ambiguities are flagged

---

## Output File Naming

For programmatic use, save outputs as:

- Tree: `{kpi_name}_tree.txt`
- JSON: `{kpi_name}_architecture.json`
- Entities: `{kpi_name}_entities.txt`

Example:
- `site_uptime_percentage_tree.txt`
- `site_uptime_percentage_architecture.json`
- `site_uptime_percentage_entities.txt`
