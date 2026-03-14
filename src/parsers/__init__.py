"""
Parsers Package.

Contains parsers for SSIS .dtsx package files.
"""
from src.parsers.ssis_dtsx import (
    SSISParser,
    Mapping,
    Transformation,
    TransformField,
    Workflow,
    Connector,
    Source,
    Target,
    DataFlowTask,
    SSISComponent,
    SSISConnectionManager,
    SSISVariable,
    SSISControlFlowTask,
    SSISPrecedenceConstraint,
    SSIS_TYPE_MAP,
    AGG_TYPE_MAP,
    COMPONENT_TYPE_MAP,
)

__all__ = [
    "SSISParser",
    "Workflow",
    "Mapping",
    "Transformation",
    "TransformField",
    "Connector",
    "Source",
    "Target",
    "DataFlowTask",
    "SSISComponent",
    "SSISConnectionManager",
    "SSISVariable",
    "SSISControlFlowTask",
    "SSISPrecedenceConstraint",
    "SSIS_TYPE_MAP",
    "AGG_TYPE_MAP",
    "COMPONENT_TYPE_MAP",
]

