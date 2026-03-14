"""
Unit tests for SSIS DTSX parser.
"""
from pathlib import Path

import pytest

from src.parsers import SSISParser, Workflow


class TestSSISParser:
    """Tests for SSISParser (SSIS DTSX parser)."""
    
    @pytest.fixture
    def parser(self):
        return SSISParser()
    
    @pytest.fixture
    def sample_dtsx_path(self):
        """Path to sample DTSX fixture."""
        return Path(__file__).parent.parent / "fixtures" / "sample_package.dtsx"
    
    def test_parser_initialization(self, parser):
        """Test parser can be instantiated."""
        assert parser is not None
    
    def test_parse_returns_workflow(self, parser, sample_dtsx_path):
        """Test parsing returns a Workflow object."""
        if not sample_dtsx_path.exists():
            pytest.skip("Sample DTSX not found")
        
        result = parser.parse(sample_dtsx_path)
        assert isinstance(result, Workflow)
    
    def test_workflow_has_mappings(self, parser, sample_dtsx_path):
        """Test parsed workflow contains data flow tasks."""
        if not sample_dtsx_path.exists():
            pytest.skip("Sample DTSX not found")
        
        workflow = parser.parse(sample_dtsx_path)
        assert len(workflow.mappings) > 0
    
    def test_workflow_has_transformations(self, parser, sample_dtsx_path):
        """Test workflow contains transformations."""
        if not sample_dtsx_path.exists():
            pytest.skip("Sample DTSX not found")
        
        workflow = parser.parse(sample_dtsx_path)
        assert len(workflow.transformations) > 0
    
    def test_conditional_split_has_condition(self, parser, sample_dtsx_path):
        """Test Conditional Split transformation extracts condition."""
        if not sample_dtsx_path.exists():
            pytest.skip("Sample DTSX not found")
        
        workflow = parser.parse(sample_dtsx_path)
        
        # Find conditional split transformation
        cond_split_tx = None
        for tx in workflow.transformations:
            if tx.type in ("Conditional Split", "Filter"):
                cond_split_tx = tx
                break
        
        if cond_split_tx:
            assert cond_split_tx.filter_condition != ""


class TestTransformation:
    """Tests for Transformation dataclass."""
    
    def test_to_dict(self):
        """Test transformation can be serialized."""
        from src.parsers.informatica_xml import Transformation
        
        tx = Transformation(
            name="test_conditional_split",
            type="Conditional Split",
            filter_condition="COL = 'X'",
        )
        
        result = tx.to_dict()
        assert result["name"] == "test_conditional_split"
        assert result["type"] == "Conditional Split"
        assert result["filter_condition"] == "COL = 'X'"


class TestMapping:
    """Tests for Mapping dataclass."""
    
    def test_get_execution_order(self):
        """Test execution order is determined correctly."""
        from src.parsers.informatica_xml import Connector, Mapping, Transformation
        
        tx1 = Transformation(name="OLE_SRC", type="OLE DB Source")
        tx2 = Transformation(name="CSPL_FILTER", type="Conditional Split")
        tx3 = Transformation(name="OLE_DEST", type="OLE DB Destination")
        
        mapping = Mapping(
            name="test_data_flow",
            transformations=[tx3, tx1, tx2],  # Out of order
            connectors=[
                Connector("OLE_SRC", "col", "CSPL_FILTER", "col"),
                Connector("CSPL_FILTER", "col", "OLE_DEST", "col"),
            ],
        )
        
        ordered = mapping.get_execution_order()
        
        # Should be: OLE_SRC -> CSPL_FILTER -> OLE_DEST
        assert ordered[0].name == "OLE_SRC"
        assert ordered[1].name == "CSPL_FILTER"
        assert ordered[2].name == "OLE_DEST"


class TestSSISVariableFields:
    """Tests for new SSISVariable fields (readonly, is_expression)."""

    def test_default_values(self):
        """Test default values for readonly and is_expression."""
        from src.parsers.ssis_dtsx import SSISVariable

        v = SSISVariable(name="MyVar", namespace="User", data_type="8")
        assert v.readonly is False
        assert v.is_expression is False
        assert v.value == ""

    def test_readonly_variable(self):
        """Test creating a readonly variable."""
        from src.parsers.ssis_dtsx import SSISVariable

        v = SSISVariable(
            name="ServerName", namespace="User", data_type="8",
            value="localhost", readonly=True,
        )
        assert v.readonly is True
        assert v.is_expression is False

    def test_expression_variable(self):
        """Test creating an expression variable."""
        from src.parsers.ssis_dtsx import SSISVariable

        v = SSISVariable(
            name="FullPath", namespace="User", data_type="8",
            value="@[User::BasePath] + @[User::FileName]",
            is_expression=True,
        )
        assert v.is_expression is True
        assert v.readonly is False


class TestSSISControlFlowTask:
    """Tests for SSISControlFlowTask dataclass."""

    def test_creation(self):
        """Test creating a control flow task."""
        from src.parsers.ssis_dtsx import SSISControlFlowTask

        task = SSISControlFlowTask(
            name="Execute Staging SQL",
            task_type="Microsoft.ExecuteSQLTask",
            description="Run staging process",
        )
        assert task.name == "Execute Staging SQL"
        assert task.task_type == "Microsoft.ExecuteSQLTask"
        assert task.disabled is False
        assert task.properties == {}

    def test_disabled_task(self):
        """Test a disabled control flow task."""
        from src.parsers.ssis_dtsx import SSISControlFlowTask

        task = SSISControlFlowTask(
            name="Old Task",
            task_type="Microsoft.ScriptTask",
            disabled=True,
            properties={"ScriptLanguage": "CSharp"},
        )
        assert task.disabled is True
        assert task.properties["ScriptLanguage"] == "CSharp"


class TestSSISPrecedenceConstraint:
    """Tests for SSISPrecedenceConstraint dataclass."""

    def test_success_constraint(self):
        """Test a success precedence constraint."""
        from src.parsers.ssis_dtsx import SSISPrecedenceConstraint

        pc = SSISPrecedenceConstraint(
            from_task="DFT Load Data",
            to_task="SQL Cleanup",
            value=0,  # Success
        )
        assert pc.from_task == "DFT Load Data"
        assert pc.to_task == "SQL Cleanup"
        assert pc.value == 0
        assert pc.eval_op == "Constraint"
        assert pc.expression == ""
        assert pc.logical_and is True

    def test_failure_constraint(self):
        """Test a failure precedence constraint."""
        from src.parsers.ssis_dtsx import SSISPrecedenceConstraint

        pc = SSISPrecedenceConstraint(
            from_task="DFT Load Data",
            to_task="Send Error Email",
            value=1,  # Failure
        )
        assert pc.value == 1

    def test_expression_constraint(self):
        """Test a constraint with an expression."""
        from src.parsers.ssis_dtsx import SSISPrecedenceConstraint

        pc = SSISPrecedenceConstraint(
            from_task="Task A",
            to_task="Task B",
            value=0,
            eval_op="ExpressionAndConstraint",
            expression='@[User::RunMode] == "FULL"',
        )
        assert pc.eval_op == "ExpressionAndConstraint"
        assert pc.expression == '@[User::RunMode] == "FULL"'


class TestSSISConnectionManagerFlatFile:
    """Tests for SSISConnectionManager flat_file_properties."""

    def test_default_no_flat_file(self):
        """Test that non-flat-file connection has no flat file properties."""
        from src.parsers.ssis_dtsx import SSISConnectionManager

        cm = SSISConnectionManager(
            name="OLEDB_Source",
            creation_name="OLEDB",
            connection_string="Data Source=server;...",
        )
        assert cm.flat_file_properties == {}

    def test_flat_file_properties(self):
        """Test flat file connection with properties."""
        from src.parsers.ssis_dtsx import SSISConnectionManager

        cm = SSISConnectionManager(
            name="CSV_Source",
            creation_name="FLATFILE",
            connection_string="C:\\data\\input.csv",
            flat_file_properties={
                "Format": "Delimited",
                "ColumnDelimiter": ",",
                "columns": [
                    {"name": "ID", "data_type": "DT_I4"},
                    {"name": "Name", "data_type": "DT_WSTR"},
                ],
            },
        )
        assert cm.flat_file_properties["Format"] == "Delimited"
        assert len(cm.flat_file_properties["columns"]) == 2

