"""
SSIS DTSX Parser — v2.0 (Production-Enhanced)

Key improvements over v1.0:
  - ConditionalSplit: branch conditions extracted from output-level properties (bug fix)
  - MergeJoin: JoinType/NumKeyColumns extracted; join_condition derived from key columns
  - Lookup: NoMatchBehavior + reference SQL extracted; join keys from JoinToReferenceColumn
  - Sort: direction (ASC/DESC) via NewSortKeyPosition + ComparisonFlags
  - Expression transpiler: 40+ patterns (IIF, NULL(), $Project::, TOKEN, FINDSTRING, etc.)
  - Script component: language + ReadOnly/ReadWrite vars extracted
  - DataConvert: cast type extracted per output column
  - GUID map expanded + case-normalised lookup
"""
import re as _re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

# Logging shim — works with or without structlog installed
try:
    from src.logging import get_logger
    logger = get_logger(__name__)
except Exception:
    import logging as _logging
    _log = _logging.getLogger(__name__)
    class _LoggerShim:
        def info(self, msg, **kw):    _log.info(f"{msg} {kw}")
        def debug(self, msg, **kw):   _log.debug(f"{msg} {kw}")
        def warning(self, msg, **kw): _log.warning(f"{msg} {kw}")
        def error(self, msg, **kw):   _log.error(f"{msg} {kw}")
    logger = _LoggerShim()

DTS_NS = "www.microsoft.com/SqlServer/Dts"
NS = {"DTS": DTS_NS}


# ─── Data Classes ──────────────────────────────────────────────────────────

@dataclass
class TransformField:
    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    length: int = 0
    port_type: str = "INPUT/OUTPUT"
    expression: str = ""
    default_value: str = ""
    description: str = ""
    agg_type: str = ""
    src_col: str = ""
    sort_position: int = 0
    sort_descending: bool = False
    cast_type: str = ""


@dataclass
class SSISConnectionManager:
    name: str
    creation_name: str
    connection_string: str = ""
    flat_file_properties: dict = field(default_factory=dict)


@dataclass
class SSISVariable:
    name: str
    namespace: str
    data_type: str
    value: str = ""
    readonly: bool = False
    is_expression: bool = False


@dataclass
class SSISControlFlowTask:
    name: str
    task_type: str
    parent_path: str = ""
    description: str = ""
    disabled: bool = False
    properties: dict = field(default_factory=dict)


@dataclass
class SSISPrecedenceConstraint:
    from_task: str
    to_task: str
    value: int = 0
    eval_op: str = "Constraint"
    expression: str = ""
    logical_and: bool = True


@dataclass
class SSISComponent:
    name: str
    component_class: str
    description: str = ""
    properties: dict = field(default_factory=dict)
    input_columns: list = field(default_factory=list)
    output_columns: list = field(default_factory=list)
    connection_manager: str = ""
    sql_query: str = ""


@dataclass
class SSISPath:
    name: str
    source_component: str
    source_output: str
    dest_component: str
    dest_input: str


@dataclass
class DataFlowTask:
    name: str
    description: str = ""
    components: list["SSISComponent"] = field(default_factory=list)
    paths: list[SSISPath] = field(default_factory=list)

    @property
    def sources(self):
        return [c for c in self.components if "Source" in c.component_class]

    @property
    def destinations(self):
        return [c for c in self.components if "Destination" in c.component_class]

    @property
    def transformations(self):
        return [c for c in self.components
                if "Source" not in c.component_class
                and "Destination" not in c.component_class
                and "RowCount" not in c.component_class]


# ─── Compatibility layer ────────────────────────────────────────────────────

@dataclass
class Connector:
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str


@dataclass
class Transformation:
    name: str
    type: str
    description: str = ""
    fields: list[TransformField] = field(default_factory=list)
    properties: dict = field(default_factory=dict)
    filter_condition: str = ""
    join_condition: str = ""
    join_type: str = "inner"
    lookup_condition: str = ""
    lookup_no_match_behavior: str = "redirect"
    lookup_reference_sql: str = ""
    sql_query: str = ""
    expression_code: str = ""
    script_code: str = ""
    script_language: str = ""
    script_read_only_vars: str = ""
    script_read_write_vars: str = ""
    output_conditions: dict[str, str] = field(default_factory=dict)
    group_by: list[str] = field(default_factory=list)
    sort_columns: list[dict] = field(default_factory=list)
    input_groups: list[str] = field(default_factory=list)
    output_groups: list[str] = field(default_factory=list)
    num_key_columns: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name, "type": self.type,
            "description": self.description,
            "fields": [vars(f) for f in self.fields],
            "properties": self.properties,
            "filter_condition": self.filter_condition,
            "join_condition": self.join_condition,
            "join_type": self.join_type,
            "lookup_condition": self.lookup_condition,
            "lookup_no_match_behavior": self.lookup_no_match_behavior,
            "lookup_reference_sql": self.lookup_reference_sql,
            "sql_query": self.sql_query,
            "expression_code": self.expression_code,
            "script_code": self.script_code,
            "script_language": self.script_language,
            "output_conditions": self.output_conditions,
            "group_by": self.group_by,
            "sort_columns": self.sort_columns,
            "num_key_columns": self.num_key_columns,
        }


@dataclass
class Source:
    name: str
    type: str
    dbdname: str = ""
    description: str = ""
    fields: list[TransformField] = field(default_factory=list)
    sql_query: str = ""
    connection_manager: str = ""


@dataclass
class Target:
    name: str
    type: str
    description: str = ""
    fields: list[TransformField] = field(default_factory=list)
    connection_manager: str = ""


@dataclass
class Mapping:
    name: str
    description: str = ""
    transformations: list[Transformation] = field(default_factory=list)
    connectors: list[Connector] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
    targets: list[Target] = field(default_factory=list)
    workflow_name: str = ""
    data_flow_task: Optional[DataFlowTask] = field(default=None, repr=False)

    def get_transformation(self, name: str) -> Optional[Transformation]:
        for tx in self.transformations:
            if tx.name == name:
                return tx
        return None

    def get_execution_order(self) -> list[Transformation]:
        tx_map = {tx.name: tx for tx in self.transformations}
        deps: dict[str, set[str]] = {tx.name: set() for tx in self.transformations}
        for conn in self.connectors:
            if conn.to_instance in deps and conn.from_instance in tx_map:
                deps[conn.to_instance].add(conn.from_instance)
        visited: set[str] = set()
        order: list[Transformation] = []

        def visit(name: str):
            if name in visited or name not in tx_map:
                return
            visited.add(name)
            for dep in deps.get(name, []):
                visit(dep)
            order.append(tx_map[name])

        for name in tx_map:
            visit(name)
        return order


@dataclass
class Workflow:
    name: str
    description: str = ""
    mappings: list[Mapping] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
    targets: list[Target] = field(default_factory=list)
    connection_managers: list[SSISConnectionManager] = field(default_factory=list)
    variables: list[SSISVariable] = field(default_factory=list)
    data_flow_tasks: list[DataFlowTask] = field(default_factory=list)
    control_flow_tasks: list[SSISControlFlowTask] = field(default_factory=list)
    precedence_constraints: list[SSISPrecedenceConstraint] = field(default_factory=list)

    @property
    def transformations(self) -> list[Transformation]:
        result = []
        for m in self.mappings:
            result.extend(m.transformations)
        return result

    @property
    def silver_mappings(self) -> list[Mapping]:
        return [m for m in self.mappings if not self._is_aggregation_task(m)]

    @property
    def gold_mappings(self) -> list[Mapping]:
        return [m for m in self.mappings if self._is_aggregation_task(m)]

    def _is_aggregation_task(self, mapping: Mapping) -> bool:
        if not mapping.data_flow_task:
            return False
        component_classes = {c.component_class for c in mapping.data_flow_task.components}
        if "Microsoft.Aggregate" in component_classes:
            return True
        if "Microsoft.Sort" in component_classes and "Microsoft.Merge" in component_classes:
            return True
        for comp in mapping.data_flow_task.components:
            if "Source" in comp.component_class:
                query = (comp.sql_query or "").lower()
                if " group by " in query or " having " in query:
                    return True
                if any(kw in query for kw in ("_transformed", "_aggregation", "_gold", "_summary")):
                    return True
        return False


# ─── Component Class ID Resolution ─────────────────────────────────────────

COMPONENT_TYPE_MAP = {
    "Microsoft.OLEDBSource": "Source",
    "Microsoft.OLEDBDestination": "Destination",
    "Microsoft.ADONETSource": "Source",
    "Microsoft.ADONETDestination": "Destination",
    "Microsoft.ExcelSource": "Source",
    "Microsoft.ExcelDestination": "Destination",
    "Microsoft.FlatFileSource": "FlatFileSource",
    "Microsoft.FlatFileDestination": "FlatFileDestination",
    "Microsoft.DerivedColumn": "DerivedColumn",
    "Microsoft.Lookup": "Lookup",
    "Microsoft.Aggregate": "Aggregate",
    "Microsoft.ConditionalSplit": "ConditionalSplit",
    "Microsoft.UnionAll": "UnionAll",
    "Microsoft.Merge": "Merge",
    "Microsoft.MergeJoin": "MergeJoin",
    "Microsoft.Multicast": "Multicast",
    "Microsoft.RowCount": "RowCount",
    "Microsoft.Sort": "Sort",
    "Microsoft.DataConvert": "DataConvert",
    "Microsoft.OLEDBCommand": "OLEDBCommand",
    "Microsoft.ScriptComponent": "Script",
    "Microsoft.BalancedDataDistributor": "BalancedDataDistributor",
    "Microsoft.FuzzyGrouping": "FuzzyGrouping",
    "Microsoft.FuzzyLookup": "FuzzyLookup",
    "Microsoft.Pivot": "Pivot",
    "Microsoft.Unpivot": "Unpivot",
    "Microsoft.CDCSource": "CDCSource",
    "Microsoft.CDCSplitter": "CDCSplitter",
}

COMPONENT_GUID_MAP: dict[str, str] = {
    "{165A526D-D5DE-47FF-96A6-F8274C19826B}": "Microsoft.OLEDBSource",
    "{4ADA7EAA-136C-4215-8098-D7A7C27FC0D1}": "Microsoft.OLEDBDestination",
    "{49928E82-9C4E-49F0-AABE-3812B82707EC}": "Microsoft.DerivedColumn",
    "{671046B0-AA63-4C9F-90E4-C06E0B710CE3}": "Microsoft.Lookup",
    "{7F88F654-4E20-4D14-84F4-AF9C925D3087}": "Microsoft.ConditionalSplit",
    "{14D43A4F-D7BD-489D-829E-6DE35750CFE4}": "Microsoft.MergeJoin",
    "{E2697D8C-70DA-42B2-8208-A19CE3A9FE41}": "Microsoft.RowCount",
    "{874F7595-FB5F-40FF-96AF-FBFF8250E3EF}": "Microsoft.ScriptComponent",
    "{5B200367-A5EF-4FCF-A40B-2C3D4ECBA212}": "Microsoft.Merge",
    "{2932025B-AB99-40F6-B5B8-783A73F80E24}": "Microsoft.Aggregate",
    "{A9C95ED8-1A9D-43BD-9C8E-BD9F3ABD1B6E}": "Microsoft.Sort",
    "{BD06A22E-BC69-4AF7-A69B-C44C2EF684BB}": "Microsoft.UnionAll",
    "{C94ACBF1-C54B-4D9C-B609-D5399B98C859}": "Microsoft.Multicast",
    "{EC139FBC-694E-490B-8EA7-B26E6D45E607}": "Microsoft.DataConvert",
    "{D7A0A7C8-143C-4A94-AB37-0E9B2B17DE93}": "Microsoft.FlatFileSource",
    "{A1DF9F6D-8EE4-4EF0-BB2E-D526130D7871}": "Microsoft.FlatFileDestination",
    "{1E0B0565-D93B-4558-905D-FE7B8BA7D85B}": "Microsoft.OLEDBCommand",
    "{AEC00FDB-23BA-4B6F-9F33-D898ADFD5A22}": "Microsoft.ADONETSource",
    "{9FB65F7E-6552-4F8A-ADEC-7BE4DA6C5C62}": "Microsoft.ADONETDestination",
    "{E08F93E1-F0DE-4E64-A8A7-1B09A9A9D4CA}": "Microsoft.ExcelSource",
    "{9A63B9BF-6EF7-4C62-8C16-C35EED56A8F4}": "Microsoft.ExcelDestination",
    "{54A4C11B-9BD4-4D9D-A948-3A0F8C2EC99D}": "Microsoft.BalancedDataDistributor",
    "{FB4AD06A-3D35-4526-84F4-0E58E0B12B85}": "Microsoft.Unpivot",
    "{B2B34D9B-D0E4-4D58-B7A6-E2A3BFAB6AC3}": "Microsoft.CDCSource",
    "{C66E4F1E-3D0A-4B1B-8C98-1B3D73ADBC00}": "Microsoft.CDCSplitter",
}

_DESCRIPTION_TYPE_MAP = {
    "OLE DB Source": "Microsoft.OLEDBSource",
    "OLE DB Destination": "Microsoft.OLEDBDestination",
    "Flat File Source": "Microsoft.FlatFileSource",
    "Flat File Destination": "Microsoft.FlatFileDestination",
    "ADO NET Source": "Microsoft.ADONETSource",
    "ADO NET Destination": "Microsoft.ADONETDestination",
    "Script Component": "Microsoft.ScriptComponent",
    "Derived Column": "Microsoft.DerivedColumn",
    "Conditional Split": "Microsoft.ConditionalSplit",
    "Aggregate": "Microsoft.Aggregate",
    "Lookup": "Microsoft.Lookup",
    "Merge Join": "Microsoft.MergeJoin",
    "Sort": "Microsoft.Sort",
    "Union All": "Microsoft.UnionAll",
    "Data Conversion": "Microsoft.DataConvert",
    "Multicast": "Microsoft.Multicast",
    "Row Count": "Microsoft.RowCount",
    "OLE DB Command": "Microsoft.OLEDBCommand",
    "Merge": "Microsoft.Merge",
}


def _resolve_component_class(class_id: str, description: str = "") -> str:
    if class_id in COMPONENT_TYPE_MAP:
        return class_id
    resolved = COMPONENT_GUID_MAP.get(class_id.upper(), "")
    if resolved:
        return resolved
    for desc_key, comp_class in _DESCRIPTION_TYPE_MAP.items():
        if desc_key.lower() in description.lower():
            return comp_class
    return class_id


# ─── SSIS Expression Transpiler v2 (40+ patterns) ──────────────────────────

_SSIS_EXPR_RULES: list[tuple[str, str]] = [
    # Variable references
    (r'\$Project::([A-Za-z_]\w*)',        r'project_\1'),
    (r'@\[User::([A-Za-z_]\w*)\]',        r'\1'),
    (r'@\[System::([A-Za-z_]\w*)\]',      r'system_\1'),
    (r'@([A-Za-z_]\w*)',                  r'\1'),
    # Type casts
    (r'\(DT_WSTR\s*,\s*\d+\)\s*\(([^)]+)\)',  r'F.col("\1").cast("string")'),
    (r'\(DT_STR\s*,\s*\d+\s*,\s*\d+\)\s*\(([^)]+)\)', r'F.col("\1").cast("string")'),
    (r'\(DT_I4\)\s*\(([^)]+)\)',           r'F.col("\1").cast("int")'),
    (r'\(DT_I8\)\s*\(([^)]+)\)',           r'F.col("\1").cast("long")'),
    (r'\(DT_R4\)\s*\(([^)]+)\)',           r'F.col("\1").cast("float")'),
    (r'\(DT_R8\)\s*\(([^)]+)\)',           r'F.col("\1").cast("double")'),
    (r'\(DT_BOOL\)\s*\(([^)]+)\)',         r'F.col("\1").cast("boolean")'),
    (r'\(DT_NUMERIC\s*,\s*(\d+)\s*,\s*(\d+)\)\s*\(([^)]+)\)',
     r'F.col("\3").cast(f"decimal(\1,\2)")'),
    (r'\(DT_DATE\)\s*\(([^)]+)\)',         r'F.to_date(F.col("\1"))'),
    (r'\(DT_DBTIMESTAMP\)\s*\(([^)]+)\)',  r'F.to_timestamp(F.col("\1"))'),
    # NULL constructors
    (r'\bNULL\s*\(DT_STR\s*,\s*\d+\s*,\s*\d+\)',  r'F.lit(None).cast("string")'),
    (r'\bNULL\s*\(DT_WSTR\s*,\s*\d+\)',            r'F.lit(None).cast("string")'),
    (r'\bNULL\s*\(DT_I4\)',                        r'F.lit(None).cast("int")'),
    (r'\bNULL\s*\(DT_I8\)',                        r'F.lit(None).cast("long")'),
    (r'\bNULL\s*\(DT_DATE\)',                      r'F.lit(None).cast("date")'),
    (r'\bNULL\s*\(DT_DBTIMESTAMP\)',               r'F.lit(None).cast("timestamp")'),
    (r'\bNULL\s*\([^)]+\)',                        r'F.lit(None)'),
    # NULL check/replace
    (r'\bISNULL\s*\(([^)]+)\)',                    r'F.col(\1).isNull()'),
    (r'\bREPLACENULL\s*\(([^,]+),([^)]+)\)',       r'F.coalesce(\1, F.lit(\2))'),
    (r'\bNULLIF\s*\(([^,]+),([^)]+)\)',            r'F.nullif(\1, \2)'),
    # Conditional
    (r'\bIIF\s*\(([^,]+),([^,]+),([^)]+)\)',       r'F.when(\1, \2).otherwise(\3)'),
    # String
    (r'\bTRIM\s*\(([^)]+)\)',               r'F.trim(\1)'),
    (r'\bLTRIM\s*\(([^)]+)\)',              r'F.ltrim(\1)'),
    (r'\bRTRIM\s*\(([^)]+)\)',              r'F.rtrim(\1)'),
    (r'\bUPPER\s*\(([^)]+)\)',              r'F.upper(\1)'),
    (r'\bLOWER\s*\(([^)]+)\)',              r'F.lower(\1)'),
    (r'\bLEN\s*\(([^)]+)\)',                r'F.length(\1)'),
    (r'\bSUBSTRING\s*\(([^,]+),([^,]+),([^)]+)\)', r'F.substring(\1, \2, \3)'),
    (r'\bLEFT\s*\(([^,]+),([^)]+)\)',       r'F.substring(\1, 1, \2)'),
    (r'\bRIGHT\s*\(([^,]+),([^)]+)\)',      r'F.expr("right(\1, \2)")'),
    (r'\bREPLACE\s*\(([^,]+),([^,]+),([^)]+)\)', r'F.regexp_replace(\1, \2, \3)'),
    (r'\bREVERSE\s*\(([^)]+)\)',            r'F.reverse(\1)'),
    (r'\bFINDSTRING\s*\(([^,]+),([^,]+),([^)]+)\)', r'F.locate(\2, \1, \3)'),
    (r'\bTOKEN\s*\(([^,]+),([^,]+),([^)]+)\)',
     r'F.split(\1, \2).getItem(int(\3) - 1)'),
    (r'\bTOKENCOUNT\s*\(([^,]+),([^)]+)\)', r'F.size(F.split(\1, \2))'),
    (r'\bCODEPOINT\s*\(([^)]+)\)',          r'F.ascii(\1)'),
    (r'\bHEX\s*\(([^)]+)\)',                r'F.hex(\1)'),
    (r'\bUNHEX\s*\(([^)]+)\)',              r'F.unhex(\1)'),
    (r'\bCONCAT\s*\(([^)]+)\)',             r'F.concat(\1)'),
    # Date/time
    (r'\bGETDATE\s*\(\)',                   r'F.current_timestamp()'),
    (r'\bGETUTCDATE\s*\(\)',               r'F.current_timestamp()'),
    (r'\bYEAR\s*\(([^)]+)\)',               r'F.year(\1)'),
    (r'\bMONTH\s*\(([^)]+)\)',              r'F.month(\1)'),
    (r'\bDAY\s*\(([^)]+)\)',                r'F.dayofmonth(\1)'),
    (r'\bHOUR\s*\(([^)]+)\)',               r'F.hour(\1)'),
    (r'\bMINUTE\s*\(([^)]+)\)',             r'F.minute(\1)'),
    (r'\bSECOND\s*\(([^)]+)\)',             r'F.second(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?year["\'"]?\s*,([^)]+)\)',   r'F.year(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?month["\'"]?\s*,([^)]+)\)',  r'F.month(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?day["\'"]?\s*,([^)]+)\)',    r'F.dayofmonth(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?hour["\'"]?\s*,([^)]+)\)',   r'F.hour(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?minute["\'"]?\s*,([^)]+)\)', r'F.minute(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?weekday["\'"]?\s*,([^)]+)\)', r'F.dayofweek(\1)'),
    (r'\bDATEPART\s*\(\s*["\'"]?week["\'"]?\s*,([^)]+)\)',   r'F.weekofyear(\1)'),
    (r'\bDATEDIFF\s*\(\s*["\'"]?day["\'"]?\s*,([^,]+),([^)]+)\)',    r'F.datediff(\2, \1)'),
    (r'\bDATEDIFF\s*\(\s*["\'"]?month["\'"]?\s*,([^,]+),([^)]+)\)',  r'F.months_between(\2, \1).cast("int")'),
    (r'\bDATEDIFF\s*\(\s*["\'"]?year["\'"]?\s*,([^,]+),([^)]+)\)',   r'(F.year(\2) - F.year(\1))'),
    (r'\bDATEADD\s*\(\s*["\'"]?day["\'"]?\s*,([^,]+),([^)]+)\)',    r'F.date_add(\2, \1)'),
    (r'\bDATEADD\s*\(\s*["\'"]?month["\'"]?\s*,([^,]+),([^)]+)\)',  r'F.add_months(\2, \1)'),
    (r'\bDATEADD\s*\(\s*["\'"]?year["\'"]?\s*,([^,]+),([^)]+)\)',   r'F.add_months(\2, \1 * 12)'),
]


def transpile_ssis_expression(expr: str) -> str:
    """Transpile SSIS expression to PySpark pseudo-code (used as LLM prompt hint)."""
    if not expr:
        return expr
    result = expr
    for pattern, replacement in _SSIS_EXPR_RULES:
        try:
            result = _re.sub(pattern, replacement, result, flags=_re.IGNORECASE)
        except Exception:
            pass
    return result


# ─── Type mappings ──────────────────────────────────────────────────────────

SSIS_TYPE_MAP = {
    "bool": "BooleanType", "i1": "ByteType", "i2": "ShortType",
    "i4": "IntegerType", "i8": "LongType", "r4": "FloatType",
    "r8": "DoubleType", "numeric": "DecimalType", "cy": "DecimalType",
    "wstr": "StringType", "str": "StringType", "bytes": "BinaryType",
    "dbDate": "DateType", "dbTime": "TimestampType",
    "dbTimeStamp": "TimestampType", "dbTimeStamp2": "TimestampType",
    "guid": "StringType", "image": "BinaryType",
    "text": "StringType", "ntext": "StringType",
}

DATACONVERT_TYPE_MAP = {
    "0": "string", "2": "short", "3": "int", "4": "long",
    "5": "float", "6": "double", "7": "timestamp",
    "8": "string", "11": "boolean", "14": "decimal",
    "16": "byte", "19": "date", "130": "string",
}

AGG_TYPE_MAP = {
    0: "GROUP_BY", 1: "COUNT_ALL", 2: "COUNT", 3: "COUNT_DISTINCT",
    4: "SUM", 5: "AVG", 6: "MIN", 7: "MAX",
}

MERGE_JOIN_TYPE_MAP = {0: "inner", 1: "left", 2: "full"}
LOOKUP_NO_MATCH_MAP = {0: "ignore", 1: "redirect", 2: "error"}


# ─── Main Parser ────────────────────────────────────────────────────────────

class SSISParser:
    """Production-enhanced SSIS .dtsx parser — v2.0."""

    def parse(self, dtsx_path: Path) -> Workflow:
        logger.info("Parsing SSIS package", path=str(dtsx_path))
        tree = ET.parse(str(dtsx_path))
        root = tree.getroot()
        pkg_name = root.get(f"{{{DTS_NS}}}ObjectName", dtsx_path.stem)
        pkg_desc = root.get(f"{{{DTS_NS}}}Description", "")
        workflow = Workflow(name=pkg_name, description=pkg_desc)
        workflow.connection_managers = self._parse_connection_managers(root)
        workflow.variables = self._parse_variables(root)
        workflow.data_flow_tasks = self._parse_data_flow_tasks(root, workflow.connection_managers)
        workflow.control_flow_tasks = self._parse_control_flow_tasks(root)
        workflow.precedence_constraints = self._parse_precedence_constraints(root)
        for dft in workflow.data_flow_tasks:
            mapping = self._dft_to_mapping(dft, workflow_name=pkg_name)
            workflow.mappings.append(mapping)
            for src in mapping.sources:
                workflow.sources.append(src)
            for tgt in mapping.targets:
                workflow.targets.append(tgt)
        logger.info("Parse complete",
                    mappings=len(workflow.mappings),
                    total_transformations=len(workflow.transformations),
                    sources=len(workflow.sources),
                    targets=len(workflow.targets))
        return workflow

    # ── Connection Managers ──────────────────────────────────────────────

    def _parse_connection_managers(self, root: ET.Element) -> list[SSISConnectionManager]:
        managers = []
        cms_elem = root.find(f"{{{DTS_NS}}}ConnectionManagers")
        if cms_elem is None:
            return managers
        for cm in cms_elem.findall(f"{{{DTS_NS}}}ConnectionManager"):
            name = cm.get(f"{{{DTS_NS}}}ObjectName", "")
            creation_name = cm.get(f"{{{DTS_NS}}}CreationName", "")
            conn_str = ""
            flat_file_props = {}
            obj_data = cm.find(f"{{{DTS_NS}}}ObjectData")
            if obj_data is not None:
                inner_cm = obj_data.find(f"{{{DTS_NS}}}ConnectionManager")
                if inner_cm is not None:
                    conn_str = inner_cm.get(f"{{{DTS_NS}}}ConnectionString", "")
                    if "FLATFILE" in creation_name.upper():
                        flat_file_props = self._extract_flat_file_properties(inner_cm)
            managers.append(SSISConnectionManager(
                name=name, creation_name=creation_name,
                connection_string=conn_str, flat_file_properties=flat_file_props))
        return managers

    def _extract_flat_file_properties(self, inner_cm: ET.Element) -> dict:
        props = {}
        for prop_elem in inner_cm.findall(f"{{{DTS_NS}}}Property"):
            prop_name = prop_elem.get(f"{{{DTS_NS}}}Name", prop_elem.get("Name", ""))
            if prop_name in ("Format", "ColumnDelimiter", "RowDelimiter",
                             "HeaderRowsToSkip", "Unicode", "CodePage",
                             "TextQualifier", "HeaderRowDelimiter"):
                props[prop_name] = prop_elem.text or ""
        columns = []
        for col_elem in inner_cm.findall(f"{{{DTS_NS}}}FlatFileColumn"):
            col_info = {
                "name": col_elem.get(f"{{{DTS_NS}}}ObjectName", ""),
                "data_type": col_elem.get(f"{{{DTS_NS}}}DataType", ""),
                "column_width": col_elem.get(f"{{{DTS_NS}}}ColumnWidth", "0"),
                "max_width": col_elem.get(f"{{{DTS_NS}}}MaximumWidth", "0"),
            }
            for cp in col_elem.findall(f"{{{DTS_NS}}}Property"):
                if cp.get(f"{{{DTS_NS}}}Name", cp.get("Name", "")) == "ColumnDelimiter":
                    col_info["delimiter"] = cp.text or ""
            columns.append(col_info)
        if columns:
            props["columns"] = columns
        return props

    # ── Variables ────────────────────────────────────────────────────────

    def _parse_variables(self, root: ET.Element) -> list[SSISVariable]:
        variables = []
        vars_elem = root.find(f"{{{DTS_NS}}}Variables")
        if vars_elem is None:
            return variables
        for var in vars_elem.findall(f"{{{DTS_NS}}}Variable"):
            name = var.get(f"{{{DTS_NS}}}ObjectName", "")
            namespace = var.get(f"{{{DTS_NS}}}Namespace", "User")
            readonly = var.get(f"{{{DTS_NS}}}ReadOnly", "False") == "True"
            val_elem = var.find(f"{{{DTS_NS}}}VariableValue")
            data_type = val_elem.get(f"{{{DTS_NS}}}DataType", "") if val_elem is not None else ""
            value = val_elem.text or "" if val_elem is not None else ""
            is_expression = False
            if val_elem is not None:
                is_expression = val_elem.get(
                    f"{{{DTS_NS}}}EvaluateAsExpression",
                    val_elem.get("EvaluateAsExpression", "False")) == "True"
            variables.append(SSISVariable(
                name=name, namespace=namespace, data_type=data_type,
                value=value, readonly=readonly, is_expression=is_expression))
        return variables

    # ── Recursive Executable Discovery ──────────────────────────────────

    _CONTAINER_CREATION_NAMES = {
        "STOCK:SEQUENCE", "STOCK:FORLOOP", "STOCK:FOREACHLOOP",
        "SSIS.Sequence.3", "SSIS.ForLoop.3", "SSIS.ForEachLoop.3",
    }

    def _find_all_executables(self, element: ET.Element, parent_path: str = "") -> list[tuple]:
        results = []
        executables = element.find(f"{{{DTS_NS}}}Executables")
        if executables is None:
            return results
        for exe in executables.findall(f"{{{DTS_NS}}}Executable"):
            name = exe.get(f"{{{DTS_NS}}}ObjectName", "")
            creation_name = exe.get(f"{{{DTS_NS}}}CreationName", "")
            current_path = f"{parent_path}\\{name}" if parent_path else name
            results.append((exe, current_path, creation_name))
            if creation_name in self._CONTAINER_CREATION_NAMES:
                results.extend(self._find_all_executables(exe, current_path))
        return results

    # ── Control Flow Tasks ───────────────────────────────────────────────

    _DATA_FLOW_CREATION_NAMES = {"Microsoft.Pipeline", "SSIS.Pipeline.3"}
    _CONTAINER_FRIENDLY_NAMES = {
        "STOCK:SEQUENCE": "SequenceContainer", "STOCK:FORLOOP": "ForLoopContainer",
        "STOCK:FOREACHLOOP": "ForEachLoopContainer",
        "SSIS.Sequence.3": "SequenceContainer", "SSIS.ForLoop.3": "ForLoopContainer",
        "SSIS.ForEachLoop.3": "ForEachLoopContainer",
    }

    def _extract_task_properties(self, exe: ET.Element, creation_name: str) -> dict:
        properties: dict = {"CreationName": creation_name}
        if creation_name in self._CONTAINER_CREATION_NAMES:
            friendly = self._CONTAINER_FRIENDLY_NAMES.get(creation_name, creation_name)
            properties["ContainerType"] = friendly
            fe_enum = exe.find(f".//{{{DTS_NS}}}ForEachEnumerator")
            if fe_enum is not None:
                properties["EnumeratorType"] = fe_enum.get(f"{{{DTS_NS}}}CreationName", "")
                obj_data = fe_enum.find(f"{{{DTS_NS}}}ObjectData")
                if obj_data is not None:
                    for child in obj_data:
                        for attr_name, attr_val in child.attrib.items():
                            short = attr_name.split("}")[-1] if "}" in attr_name else attr_name
                            properties[f"Enumerator_{short}"] = attr_val
            var_mappings = exe.findall(f".//{{{DTS_NS}}}ForEachVariableMapping")
            if var_mappings:
                mappings = [{"variable": vm.get(f"{{{DTS_NS}}}VariableName", ""),
                             "index": vm.get(f"{{{DTS_NS}}}ValueIndex", "")}
                            for vm in var_mappings]
                properties["VariableMappings"] = mappings
            for prop_name in ("InitExpression", "EvalExpression", "AssignExpression"):
                val = exe.get(f"{{{DTS_NS}}}{prop_name}", "")
                if val:
                    properties[prop_name] = val
        obj_data = exe.find(f"{{{DTS_NS}}}ObjectData")
        if obj_data is not None:
            for child in obj_data:
                for attr_name, attr_val in child.attrib.items():
                    short_name = attr_name.split("}")[-1] if "}" in attr_name else attr_name
                    properties[short_name] = attr_val
                if child.text and child.text.strip():
                    tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    properties[f"{tag}_text"] = child.text.strip()
        return properties

    def _parse_control_flow_tasks(self, root: ET.Element) -> list[SSISControlFlowTask]:
        tasks = []
        for exe, hierarchy_path, creation_name in self._find_all_executables(root):
            if creation_name in self._DATA_FLOW_CREATION_NAMES:
                continue
            task_name = exe.get(f"{{{DTS_NS}}}ObjectName", "")
            task_desc = exe.get(f"{{{DTS_NS}}}Description", "")
            disabled = exe.get(f"{{{DTS_NS}}}Disabled", "False") == "True"
            parts = hierarchy_path.rsplit("\\", 1)
            parent_path = parts[0] if len(parts) > 1 else ""
            properties = self._extract_task_properties(exe, creation_name)
            tasks.append(SSISControlFlowTask(
                name=task_name, task_type=creation_name,
                parent_path=parent_path, description=task_desc,
                disabled=disabled, properties=properties))
        return tasks

    # ── Precedence Constraints ───────────────────────────────────────────

    def _parse_precedence_constraints(self, root: ET.Element) -> list[SSISPrecedenceConstraint]:
        constraints = []
        self._collect_precedence_constraints(root, constraints)
        return constraints

    def _collect_precedence_constraints(self, element: ET.Element, constraints: list) -> None:
        pc_container = element.find(f"{{{DTS_NS}}}PrecedenceConstraints")
        if pc_container is not None:
            for pc in pc_container.findall(f"{{{DTS_NS}}}PrecedenceConstraint"):
                value = int(pc.get(f"{{{DTS_NS}}}Value", "0"))
                eval_op = pc.get(f"{{{DTS_NS}}}EvalOp", "Constraint")
                expression = pc.get(f"{{{DTS_NS}}}Expression", "")
                logical_and = pc.get(f"{{{DTS_NS}}}LogicalAnd", "True").lower() != "false"
                from_ref = pc.get(f"{{{DTS_NS}}}From", "")
                to_ref = pc.get(f"{{{DTS_NS}}}To", "")
                from_task = from_ref.replace("\\", "/").split("/")[-1] if from_ref else ""
                to_task = to_ref.replace("\\", "/").split("/")[-1] if to_ref else ""
                constraints.append(SSISPrecedenceConstraint(
                    from_task=from_task, to_task=to_task, value=value,
                    eval_op=eval_op, expression=expression, logical_and=logical_and))
        executables = element.find(f"{{{DTS_NS}}}Executables")
        if executables is not None:
            for exe in executables.findall(f"{{{DTS_NS}}}Executable"):
                if exe.get(f"{{{DTS_NS}}}CreationName", "") in self._CONTAINER_CREATION_NAMES:
                    self._collect_precedence_constraints(exe, constraints)

    # ── Data Flow Tasks ──────────────────────────────────────────────────

    def _parse_data_flow_tasks(self, root: ET.Element,
                                conn_managers: list[SSISConnectionManager]) -> list[DataFlowTask]:
        tasks = []
        conn_map = {cm.name: cm for cm in conn_managers}
        for exe, hierarchy_path, creation_name in self._find_all_executables(root):
            if creation_name not in self._DATA_FLOW_CREATION_NAMES:
                continue
            dft_name = exe.get(f"{{{DTS_NS}}}ObjectName", "")
            dft_desc = exe.get(f"{{{DTS_NS}}}Description", "")
            dft = DataFlowTask(name=dft_name, description=dft_desc)
            obj_data = exe.find(f"{{{DTS_NS}}}ObjectData")
            if obj_data is None:
                continue
            pipeline = obj_data.find("pipeline")
            if pipeline is None:
                continue
            components_elem = pipeline.find("components")
            if components_elem is not None:
                for comp_elem in components_elem.findall("component"):
                    comp = self._parse_component(comp_elem, conn_map)
                    dft.components.append(comp)
            paths_elem = pipeline.find("paths")
            if paths_elem is not None:
                for path_elem in paths_elem.findall("path"):
                    path = self._parse_path(path_elem, dft.components)
                    if path:
                        dft.paths.append(path)
            tasks.append(dft)
            logger.debug("Found Data Flow Task", name=dft_name,
                         hierarchy=hierarchy_path,
                         components=len(dft.components), paths=len(dft.paths))
        return tasks

    # ── Component Parsing ────────────────────────────────────────────────

    def _parse_component(self, comp_elem: ET.Element, conn_map: dict) -> SSISComponent:
        name = comp_elem.get("name", "")
        raw_class_id = comp_elem.get("componentClassID", "")
        description = comp_elem.get("description", "")
        class_id = _resolve_component_class(raw_class_id, description)
        comp = SSISComponent(name=name, component_class=class_id, description=description)

        # Properties
        props_elem = comp_elem.find("properties")
        if props_elem is not None:
            for prop in props_elem.findall("property"):
                prop_name = prop.get("name", "")
                is_array = prop.get("isArray", "false") == "true"
                if is_array:
                    array_elements = []
                    elements_elem = prop.find("arrayElements")
                    if elements_elem is not None:
                        for elem in elements_elem.findall("arrayElement"):
                            array_elements.append(elem.text or "")
                    prop_value = "\n".join(array_elements)
                else:
                    prop_value = prop.text or ""
                comp.properties[prop_name] = prop_value
                if prop_name in ("SqlCommand", "SqlCommandParam") and prop_value and not comp.sql_query:
                    comp.sql_query = prop_value
                elif prop_name == "OpenRowset" and prop_value and not comp.sql_query:
                    comp.sql_query = prop_value

        # Connection reference
        conns_elem = comp_elem.find("connections")
        if conns_elem is not None:
            for conn in conns_elem.findall("connection"):
                cm_ref = conn.get("connectionManagerRefId", "")
                if "[" in cm_ref and "]" in cm_ref:
                    comp.connection_manager = cm_ref.split("[")[1].rstrip("]")

        # Input columns
        inputs_elem = comp_elem.find("inputs")
        if inputs_elem is not None:
            for inp in inputs_elem.findall("input"):
                inp_name = inp.get("name", "")
                inp_cols = inp.find("inputColumns")
                if inp_cols is not None:
                    for col in inp_cols.findall("inputColumn"):
                        col_info = self._parse_column(col, is_input=True)
                        col_info["input_name"] = inp_name
                        comp.input_columns.append(col_info)

        # Output columns — CRITICAL FIX: also capture output-level properties
        outputs_elem = comp_elem.find("outputs")
        if outputs_elem is not None:
            for out in outputs_elem.findall("output"):
                out_name = out.get("name", "")
                is_error = out.get("isErrorOut", "false").lower() == "true"

                # Extract output-level condition (ConditionalSplit stores conditions HERE)
                out_condition = ""
                out_friendly = ""
                eval_order = 0
                out_props_elem = out.find("properties")
                if out_props_elem is not None:
                    for p in out_props_elem.findall("property"):
                        pname = p.get("name", "")
                        if pname == "Expression":
                            out_condition = p.text or ""
                        elif pname == "FriendlyExpression":
                            out_friendly = p.text or ""
                        elif pname == "EvaluationOrder":
                            try:
                                eval_order = int(p.text or "0")
                            except (ValueError, TypeError):
                                pass

                out_cols = out.find("outputColumns")
                if out_cols is not None:
                    for col in out_cols.findall("outputColumn"):
                        col_info = self._parse_column(col, is_input=False)
                        col_info["output_name"] = out_name
                        col_info["is_error_output"] = is_error
                        col_info["output_condition"] = out_condition
                        col_info["output_friendly_condition"] = out_friendly
                        col_info["output_eval_order"] = eval_order
                        comp.output_columns.append(col_info)
                else:
                    # No output columns but output has a condition → sentinel record
                    if out_condition or out_friendly:
                        comp.output_columns.append({
                            "name": "__branch__",
                            "dataType": "", "length": 0, "precision": 0, "scale": 0,
                            "output_name": out_name,
                            "is_error_output": is_error,
                            "output_condition": out_condition,
                            "output_friendly_condition": out_friendly,
                            "output_eval_order": eval_order,
                        })
        return comp

    def _parse_column(self, col_elem: ET.Element, is_input: bool) -> dict:
        info = {
            "name": col_elem.get("name", col_elem.get("cachedName", "")),
            "dataType": col_elem.get("dataType", col_elem.get("cachedDataType", "")),
            "length": int(col_elem.get("length", col_elem.get("cachedLength", "0")) or "0"),
            "precision": int(col_elem.get("precision", col_elem.get("cachedPrecision", "0")) or "0"),
            "scale": int(col_elem.get("scale", col_elem.get("cachedScale", "0")) or "0"),
        }
        props_elem = col_elem.find("properties")
        if props_elem is not None:
            for prop in props_elem.findall("property"):
                info[prop.get("name", "")] = prop.text or ""
        return info

    # ── Path Parsing ─────────────────────────────────────────────────────

    def _parse_path(self, path_elem: ET.Element,
                    components: list[SSISComponent]) -> Optional[SSISPath]:
        name = path_elem.get("name", "")
        start_id = path_elem.get("startId", "")
        end_id = path_elem.get("endId", "")
        src_comp, src_output = self._parse_ref_id(start_id)
        dst_comp, dst_input = self._parse_ref_id(end_id)
        if src_comp and dst_comp:
            return SSISPath(name=name, source_component=src_comp, source_output=src_output,
                            dest_component=dst_comp, dest_input=dst_input)
        return None

    def _parse_ref_id(self, ref_id: str) -> tuple[str, str]:
        if not ref_id:
            return "", ""
        parts = ref_id.replace("\\", "/").split("/")
        if len(parts) < 2:
            return "", ""
        last = parts[-1]
        if "." in last:
            comp_name, port_info = last.split(".", 1)
            return comp_name, port_info
        return last, ""

    # ── DFT → Mapping Conversion ─────────────────────────────────────────

    def _dft_to_mapping(self, dft: DataFlowTask, workflow_name: str = "") -> Mapping:
        mapping = Mapping(name=dft.name, description=dft.description,
                          data_flow_task=dft, workflow_name=workflow_name)

        for comp in dft.components:
            if "Source" in comp.component_class:
                fields = [
                    TransformField(
                        name=c["name"], datatype=c.get("dataType", ""),
                        precision=c.get("precision", 0), scale=c.get("scale", 0),
                        length=c.get("length", 0), port_type="OUTPUT")
                    for c in comp.output_columns
                    if c.get("name") and c.get("name") != "__branch__"
                ]
                mapping.sources.append(Source(
                    name=comp.name, type=comp.component_class,
                    description=comp.description, fields=fields,
                    sql_query=comp.sql_query, connection_manager=comp.connection_manager))

        for comp in dft.components:
            if "Destination" in comp.component_class:
                fields = [
                    TransformField(name=c["name"], datatype=c.get("dataType", ""), port_type="INPUT")
                    for c in comp.input_columns]
                table_name = comp.properties.get("OpenRowset", comp.name)
                mapping.targets.append(Target(
                    name=table_name, type=comp.component_class,
                    description=comp.description, fields=fields,
                    connection_manager=comp.connection_manager))

        for comp in dft.components:
            comp_type = COMPONENT_TYPE_MAP.get(comp.component_class, comp.component_class)
            if comp_type in ("Source", "Destination", "RowCount"):
                continue
            tx = self._build_transformation(comp, comp_type)
            mapping.transformations.append(tx)

        for path in dft.paths:
            mapping.connectors.append(Connector(
                from_instance=path.source_component, from_field=path.source_output,
                to_instance=path.dest_component, to_field=path.dest_input))

        return mapping

    def _build_transformation(self, comp: SSISComponent, comp_type: str) -> Transformation:
        """Build an enriched Transformation from an SSISComponent."""
        fields = []
        for c in comp.input_columns:
            fields.append(TransformField(
                name=c.get("name", ""), datatype=c.get("dataType", ""),
                precision=c.get("precision", 0), scale=c.get("scale", 0),
                port_type="INPUT", expression=c.get("Expression", "")))

        for c in comp.output_columns:
            if c.get("name") == "__branch__":
                continue
            agg_type_str = ""
            src_col_str = ""
            sort_position = 0
            sort_descending = False
            cast_type = ""

            if comp_type == "Aggregate":
                agg_type_int = int(c.get("AggregationType", "-1"))
                agg_type_str = AGG_TYPE_MAP.get(agg_type_int, "")
                src_col_str = c.get("CopyFromReferenceColumn", "") or c.get("name", "")
            elif comp_type == "Sort":
                try:
                    sort_position = int(c.get("NewSortKeyPosition", "0"))
                except (ValueError, TypeError):
                    sort_position = 0
                try:
                    flags = int(c.get("ComparisonFlags", "0"))
                    sort_descending = bool(flags & 1)
                except (ValueError, TypeError):
                    sort_descending = False
            elif comp_type == "DataConvert":
                raw_dt = c.get("dataType", "")
                cast_type = DATACONVERT_TYPE_MAP.get(str(raw_dt), raw_dt)

            fields.append(TransformField(
                name=c.get("name", ""), datatype=c.get("dataType", ""),
                precision=c.get("precision", 0), scale=c.get("scale", 0),
                port_type="OUTPUT",
                expression=c.get("Expression", ""),
                description=c.get("FriendlyExpression", ""),
                agg_type=agg_type_str, src_col=src_col_str,
                sort_position=sort_position, sort_descending=sort_descending,
                cast_type=cast_type))

        # Sort columns
        sort_columns: list[dict] = []
        if comp_type == "Sort":
            sortable = [(f.name, f.sort_position, f.sort_descending)
                        for f in fields if f.port_type == "OUTPUT" and f.sort_position > 0]
            sort_columns = [{"column": name, "ascending": not desc}
                            for name, pos, desc in sorted(sortable, key=lambda x: x[1])]

        # Aggregate group-by
        group_by: list[str] = []
        if comp_type == "Aggregate":
            for c in comp.output_columns:
                if c.get("name") == "__branch__":
                    continue
                if int(c.get("AggregationType", "-1")) == 0:
                    group_by.append(c["name"])

        # Lookup
        lookup_condition = ""
        lookup_no_match = "redirect"
        lookup_ref_sql = ""
        if comp_type == "Lookup":
            join_parts = [f"{c['name']} = {c.get('JoinToReferenceColumn', '')}"
                          for c in comp.input_columns if c.get("JoinToReferenceColumn", "")]
            lookup_condition = " AND ".join(join_parts)
            no_match_int = int(comp.properties.get("NoMatchBehavior", "1"))
            lookup_no_match = LOOKUP_NO_MATCH_MAP.get(no_match_int, "redirect")
            lookup_ref_sql = (comp.properties.get("SqlCommand", "")
                              or comp.properties.get("SqlCommandParam", ""))

        # MergeJoin
        join_condition = ""
        join_type = "inner"
        num_key_columns = 0
        if comp_type == "MergeJoin":
            jt_int = int(comp.properties.get("JoinType", "0"))
            join_type = MERGE_JOIN_TYPE_MAP.get(jt_int, "inner")
            try:
                num_key_columns = int(comp.properties.get("NumKeyColumns", "1"))
            except (ValueError, TypeError):
                num_key_columns = 1
            left_cols = [c for c in comp.input_columns if "Left" in c.get("input_name", "")]
            right_cols = [c for c in comp.input_columns if "Right" in c.get("input_name", "")]
            if not left_cols:
                left_cols = comp.input_columns[:len(comp.input_columns)//2]
            if not right_cols:
                right_cols = comp.input_columns[len(comp.input_columns)//2:]
            if left_cols and right_cols and num_key_columns > 0:
                key_pairs = [
                    f"{left_cols[i]['name']} = {right_cols[i]['name']}"
                    for i in range(min(num_key_columns, len(left_cols), len(right_cols)))
                ]
                join_condition = " AND ".join(key_pairs)
            # Fallback: JoinToReferenceColumn
            if not join_condition:
                join_parts = [f"{c['name']} = {c.get('JoinToReferenceColumn', '')}"
                              for c in comp.input_columns if c.get("JoinToReferenceColumn", "")]
                join_condition = " AND ".join(join_parts)

        # ConditionalSplit — FIXED: read from output-level properties
        output_conditions: dict[str, str] = {}
        if comp_type == "ConditionalSplit":
            seen: dict[str, tuple[str, int]] = {}
            for c in comp.output_columns:
                out_name = c.get("output_name", "")
                cond = c.get("output_friendly_condition", "") or c.get("output_condition", "")
                eval_order = c.get("output_eval_order", 0)
                is_error = c.get("is_error_output", False)
                if cond and out_name and "Default Output" not in out_name and not is_error:
                    if out_name not in seen:
                        seen[out_name] = (cond, eval_order)
            output_conditions = {
                name: transpile_ssis_expression(cond)
                for name, (cond, _) in sorted(seen.items(), key=lambda x: x[1][1])
            }

        # Script component
        script_code = ""
        script_language = ""
        script_read_only = ""
        script_read_write = ""
        if comp_type == "Script":
            script_code = comp.properties.get("SourceCode", "")
            script_language = comp.properties.get("ScriptLanguage", "CSharp")
            script_read_only = comp.properties.get("ReadOnlyVariables", "")
            script_read_write = comp.properties.get("ReadWriteVariables", "")

        # DerivedColumn expressions
        expression_code = ""
        if comp_type == "DerivedColumn":
            expressions = []
            for c in comp.output_columns:
                if c.get("name") == "__branch__":
                    continue
                raw_expr = c.get("Expression", "") or c.get("FriendlyExpression", "")
                if raw_expr:
                    translated = transpile_ssis_expression(raw_expr)
                    col_name = c.get("name", "")
                    if translated != raw_expr:
                        expressions.append(f"{col_name}: {translated}  # SSIS: {raw_expr}")
                    else:
                        expressions.append(f"{col_name}: {raw_expr}")
            expression_code = "\n".join(expressions)

        return Transformation(
            name=comp.name, type=comp_type, description=comp.description,
            fields=fields, properties=dict(comp.properties),
            sql_query=comp.sql_query,
            expression_code=expression_code,
            script_code=script_code, script_language=script_language,
            script_read_only_vars=script_read_only, script_read_write_vars=script_read_write,
            output_conditions=output_conditions,
            group_by=group_by, sort_columns=sort_columns,
            lookup_condition=lookup_condition, lookup_no_match_behavior=lookup_no_match,
            lookup_reference_sql=lookup_ref_sql,
            join_condition=join_condition, join_type=join_type,
            num_key_columns=num_key_columns)
