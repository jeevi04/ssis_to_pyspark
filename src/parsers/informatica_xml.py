"""
Informatica XML Parser.

Parses Informatica PowerCenter XML exports and extracts workflow,
mapping, and transformation metadata.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from lxml import etree

from src.logging import get_logger

logger = get_logger(__name__)


@dataclass
class TransformField:
    """Represents a field/port in a transformation."""
    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    port_type: str = "INPUT/OUTPUT"  # INPUT, OUTPUT, INPUT/OUTPUT, VARIABLE
    expression: str = ""
    default_value: str = ""
    description: str = ""


@dataclass
class Source:
    """Represents an Informatica source."""
    name: str
    type: str
    dbdname: str = ""
    description: str = ""
    fields: list[TransformField] = field(default_factory=list)


@dataclass
class Target:
    """Represents an Informatica target."""
    name: str
    type: str
    description: str = ""
    fields: list[TransformField] = field(default_factory=list)


@dataclass
class Transformation:
    """Represents an Informatica transformation."""
    name: str
    type: str  # Source Qualifier, Expression, Joiner, Lookup, Filter, etc.
    description: str = ""
    fields: list[TransformField] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
    
    # Type-specific attributes
    filter_condition: str = ""
    join_condition: str = ""
    lookup_condition: str = ""
    expression_code: str = ""
    group_by: list[str] = field(default_factory=list)
    sql_query: str = ""
    
    # Connectivity
    input_groups: list[str] = field(default_factory=list)
    output_groups: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "fields": [vars(f) for f in self.fields],
            "properties": self.properties,
            "filter_condition": self.filter_condition,
            "join_condition": self.join_condition,
            "lookup_condition": self.lookup_condition,
            "expression_code": self.expression_code,
            "group_by": self.group_by,
            "sql_query": self.sql_query,
        }


@dataclass
class Connector:
    """Represents a connection between transformations."""
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str


@dataclass
class Mapping:
    """Represents an Informatica mapping."""
    name: str
    description: str = ""
    transformations: list[Transformation] = field(default_factory=list)
    connectors: list[Connector] = field(default_factory=list)
    
    def get_transformation(self, name: str) -> Optional[Transformation]:
        """Get transformation by name."""
        for tx in self.transformations:
            if tx.name == name:
                return tx
        return None
    
    def get_execution_order(self) -> list[Transformation]:
        """
        Get transformations in execution order based on data flow.
        
        Returns:
            Transformations sorted by dependency order
        """
        # Build dependency graph
        dependencies: dict[str, set[str]] = {tx.name: set() for tx in self.transformations}
        
        for conn in self.connectors:
            if conn.to_instance in dependencies:
                dependencies[conn.to_instance].add(conn.from_instance)
        
        # Topological sort
        result = []
        visited = set()
        
        def visit(name: str):
            if name in visited:
                return
            visited.add(name)
            for dep in dependencies.get(name, []):
                visit(dep)
            tx = self.get_transformation(name)
            if tx:
                result.append(tx)
        
        for name in dependencies:
            visit(name)
        
        return result


@dataclass
class Workflow:
    """Represents an Informatica workflow."""
    name: str
    description: str = ""
    mappings: list[Mapping] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
    targets: list[Target] = field(default_factory=list)
    
    @property
    def transformations(self) -> list[Transformation]:
        """Get all transformations across all mappings."""
        all_tx = []
        for mapping in self.mappings:
            all_tx.extend(mapping.transformations)
        return all_tx


class InformaticaParser:
    """
    Parser for Informatica PowerCenter XML exports.
    
    Supports:
    - Workflow XML exports
    - Mapping XML exports
    - Repository XML exports (POWERMART format)
    """
    
    # Normalize transformation type names
    TRANSFORMATION_TYPES = {
        "Source Qualifier": "source_qualifier",
        "Expression": "expression",
        "Joiner": "joiner",
        "Joiner Transformation": "joiner",
        "Lookup Procedure": "lookup",
        "Lookup": "lookup",
        "Filter": "filter",
        "Aggregator": "aggregator",
        "Router": "router",
        "Sorter": "sorter",
        "Rank": "rank",
        "Sequence Generator": "sequence",
        "Normalizer": "normalizer",
        "Update Strategy": "update_strategy",
        "Union": "union",
    }
    
    def parse(self, xml_path: Path) -> Workflow:
        """
        Parse an Informatica XML export file.
        
        Args:
            xml_path: Path to the XML file
            
        Returns:
            Workflow object containing all parsed metadata
        """
        logger.info("Parsing Informatica XML", path=str(xml_path))
        
        tree = etree.parse(str(xml_path))
        root = tree.getroot()
        
        # Determine export type and parse accordingly
        if root.tag in ("POWERMART", "REPOSITORY"):
            workflow = self._parse_repository_export(root)
        elif root.tag == "WORKFLOW":
            workflow = self._parse_workflow_element(root)
        elif root.tag == "MAPPING":
            mapping = self._parse_mapping(root)
            workflow = Workflow(
                name=mapping.name,
                mappings=[mapping],
                sources=self._extract_sources_detailed(root),
                targets=self._extract_targets_detailed(root),
            )
        else:
            # Try to find embedded content
            workflow = self._parse_generic(root)
        
        logger.info(
            "Parsing complete",
            workflow=workflow.name,
            mappings=len(workflow.mappings),
            transformations=len(workflow.transformations),
        )
        
        return workflow
    
    def _parse_repository_export(self, root: etree._Element) -> Workflow:
        """Parse a POWERMART/REPOSITORY format export."""
        workflow_name = "Imported Workflow"
        mappings = []
        sources = []
        targets = []
        
        for folder in root.iter("FOLDER"):
            # Parse mappings
            for mapping_elem in folder.iter("MAPPING"):
                mapping = self._parse_mapping(mapping_elem)
                mappings.append(mapping)
            
            # Extract sources and targets
            sources.extend(self._extract_sources_detailed(folder))
            targets.extend(self._extract_targets_detailed(folder))
            
            # Get workflow name
            for wf in folder.iter("WORKFLOW"):
                workflow_name = wf.get("NAME", workflow_name)
        
        return Workflow(
            name=workflow_name,
            mappings=mappings,
            sources=sources,
            targets=targets,
        )
    
    def _parse_workflow_element(self, elem: etree._Element) -> Workflow:
        """Parse a WORKFLOW element."""
        return Workflow(
            name=elem.get("NAME", "Unknown"),
            description=elem.get("DESCRIPTION", ""),
        )
    
    def _parse_mapping(self, elem: etree._Element) -> Mapping:
        """Parse a MAPPING element."""
        transformations = []
        connectors = []
        
        # Parse transformations
        for tx_elem in elem.iter("TRANSFORMATION"):
            tx = self._parse_transformation(tx_elem)
            if tx:
                transformations.append(tx)
        
        # Parse connectors
        for conn_elem in elem.iter("CONNECTOR"):
            connectors.append(Connector(
                from_instance=conn_elem.get("FROMINSTANCE", ""),
                from_field=conn_elem.get("FROMFIELD", ""),
                to_instance=conn_elem.get("TOINSTANCE", ""),
                to_field=conn_elem.get("TOFIELD", ""),
            ))
        
        return Mapping(
            name=elem.get("NAME", "Unknown"),
            description=elem.get("DESCRIPTION", ""),
            transformations=transformations,
            connectors=connectors,
        )
    
    def _parse_transformation(self, elem: etree._Element) -> Optional[Transformation]:
        """Parse a TRANSFORMATION element."""
        tx_type = elem.get("TYPE", "Unknown")
        name = elem.get("NAME", "Unknown")
        
        # Parse fields
        fields = []
        for field_elem in elem.iter("TRANSFORMFIELD"):
            fields.append(TransformField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", "string"),
                precision=int(field_elem.get("PRECISION", 0) or 0),
                scale=int(field_elem.get("SCALE", 0) or 0),
                port_type=field_elem.get("PORTTYPE", "INPUT/OUTPUT"),
                expression=field_elem.get("EXPRESSION", ""),
                default_value=field_elem.get("DEFAULTVALUE", ""),
                description=field_elem.get("DESCRIPTION", ""),
            ))
        
        # Parse properties
        properties = {}
        for attr in elem.iter("TABLEATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")
            if attr_name:
                properties[attr_name] = attr_value
        
        # Create transformation
        tx = Transformation(
            name=name,
            type=tx_type,
            description=elem.get("DESCRIPTION", ""),
            fields=fields,
            properties=properties,
        )
        
        # Extract type-specific information
        if tx_type == "Filter":
            tx.filter_condition = properties.get("Filter Condition", "")
        
        elif tx_type in ("Joiner", "Joiner Transformation"):
            tx.join_condition = properties.get("Join Condition", "")
        
        elif tx_type in ("Lookup Procedure", "Lookup"):
            tx.lookup_condition = properties.get("Lookup condition", "")
        
        elif tx_type == "Expression":
            expressions = [f.expression for f in fields if f.expression]
            tx.expression_code = "\n".join(expressions)
        
        elif tx_type == "Aggregator":
            tx.group_by = [f.name for f in fields if f.port_type == "GROUP BY"]
        
        elif tx_type == "Source Qualifier":
            tx.sql_query = properties.get("Sql Query", "")
        
        return tx
    
    def _parse_generic(self, root: etree._Element) -> Workflow:
        """Attempt to parse unknown XML structure."""
        mappings = []
        
        for mapping_elem in root.iter("MAPPING"):
            mapping = self._parse_mapping(mapping_elem)
            mappings.append(mapping)
        
        return Workflow(
            name="Parsed Workflow",
            mappings=mappings,
            sources=self._extract_sources_detailed(root),
            targets=self._extract_targets_detailed(root),
        )
    
    def _extract_sources_detailed(self, root: etree._Element) -> list[Source]:
        """Extract detailed source information."""
        sources = []
        for elem in root.iter("SOURCE"):
            name = elem.get("NAME")
            if not name:
                continue
            
            fields = []
            for field_elem in elem.iter("SOURCEFIELD"):
                fields.append(TransformField(
                    name=field_elem.get("NAME", ""),
                    datatype=field_elem.get("DATATYPE", "string"),
                    precision=int(field_elem.get("PRECISION", 0) or 0),
                    scale=int(field_elem.get("SCALE", 0) or 0),
                    description=field_elem.get("DESCRIPTION", ""),
                ))
            
            sources.append(Source(
                name=name,
                type=elem.get("DATABASETYPE", "Unknown"),
                dbdname=elem.get("DBDNAME", ""),
                description=elem.get("DESCRIPTION", ""),
                fields=fields
            ))
        return sources

    def _extract_targets_detailed(self, root: etree._Element) -> list[Target]:
        """Extract detailed target information."""
        targets = []
        for elem in root.iter("TARGET"):
            name = elem.get("NAME")
            if not name:
                continue
            
            fields = []
            for field_elem in elem.iter("TARGETFIELD"):
                fields.append(TransformField(
                    name=field_elem.get("NAME", ""),
                    datatype=field_elem.get("DATATYPE", "string"),
                    precision=int(field_elem.get("PRECISION", 0) or 0),
                    scale=int(field_elem.get("SCALE", 0) or 0),
                    description=field_elem.get("DESCRIPTION", ""),
                ))
            
            targets.append(Target(
                name=name,
                type=elem.get("DATABASETYPE", "Unknown"),
                description=elem.get("DESCRIPTION", ""),
                fields=fields
            ))
        return targets

    def _extract_sources(self, root: etree._Element) -> list[str]:
        """Extract source names."""
        return list(set(
            elem.get("NAME") for elem in root.iter("SOURCE")
            if elem.get("NAME")
        ))
    
    def _extract_targets(self, root: etree._Element) -> list[str]:
        """Extract target names."""
        return list(set(
            elem.get("NAME") for elem in root.iter("TARGET")
            if elem.get("NAME")
        ))
