# SSIS to PySpark Converter - Design Document

## Document Information
| Attribute | Value |
|-----------|-------|
| Document ID | DES-001 |
| Version | 1.0 |
| Last Updated | 2025-01-02 |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           CLI Interface                              │
│                            (src/cli.py)                              │
│  - convert: Convert single mapping/workflow                          │
│  - batch: Batch convert multiple files                               │
│  - validate: Validate conversion output                              │
│  - analyze: Analyze SSIS packages                              │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          XML Parser Layer                            │
│                    (src/parsers/ssis_dtsx.py)                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────┐   │
│  │ Mapping Parser  │  │ Workflow Parser  │  │  Session Parser   │   │
│  │                 │  │                  │  │                   │   │
│  │ - Sources       │  │ - Tasks          │  │ - Config          │   │
│  │ - Targets       │  │ - Links          │  │ - Parameters      │   │
│  │ - Transforms    │  │ - Variables      │  │ - Connections     │   │
│  │ - Expressions   │  │ - Events         │  │ - Partitioning    │   │
│  └─────────────────┘  └──────────────────┘  └───────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Domain Knowledge Layer                         │
│                   (knowledge/transformations/)                       │
│  - Transformation semantics and behavior                             │
│  - Function mappings and equivalents                                 │
│  - Type conversion rules                                             │
│  - Best practices and patterns                                       │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        LLM Provider Layer                            │
│                          (src/llm/)                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────┐   │
│  │    Anthropic    │  │     Gemini       │  │      Ollama       │   │
│  │    (Claude)     │  │   (Google)       │  │   (Local LLM)     │   │
│  └─────────────────┘  └──────────────────┘  └───────────────────┘   │
│                              │                                       │
│                    ┌─────────┴─────────┐                            │
│                    │  Prompt Manager   │                            │
│                    │  - System prompts │                            │
│                    │  - Templates      │                            │
│                    │  - Context        │                            │
│                    └───────────────────┘                            │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Generator Layer                               │
│                      (src/generators/)                               │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────┐   │
│  │ PySpark Output  │  │ Workflow Output  │  │  Report Output    │   │
│  │                 │  │                  │  │                   │   │
│  │ - DataFrame ops │  │ - Airflow DAGs   │  │ - Conversion log  │   │
│  │ - Functions     │  │ - Prefect flows  │  │ - Lineage report  │   │
│  │ - Schema        │  │ - Dependencies   │  │ - Manual items    │   │
│  └─────────────────┘  └──────────────────┘  └───────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Output Files                                 │
│                          (output/)                                   │
│  - Generated PySpark scripts (.py)                                   │
│  - Workflow orchestration files                                      │
│  - Conversion reports                                                │
│  - Data lineage documentation                                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component Design

### 1. XML Parser Layer

#### 1.1 Mapping Parser (`src/parsers/mapping_parser.py`)

**Responsibilities:**
- Parse SSIS DTSX package files
- Extract source and target definitions
- Build transformation dependency graph
- Extract expressions and business logic

**Key Classes:**
```python
class MappingParser:
    """Main parser for SSIS DTSX package files."""
    def parse(self, xml_path: str) -> Mapping
    def extract_sources(self, root: Element) -> List[Source]
    def extract_targets(self, root: Element) -> List[Target]
    def extract_transformations(self, root: Element) -> List[Transformation]
    def build_dependency_graph(self, transformations: List[Transformation]) -> DAG

class Mapping:
    """Represents a complete SSIS Data Flow Task."""
    name: str
    sources: List[Source]
    targets: List[Target]
    transformations: List[Transformation]
    dependency_graph: DAG

class Transformation:
    """Represents a single transformation in a mapping."""
    name: str
    type: TransformationType
    ports: List[Port]
    properties: Dict[str, Any]
    expressions: List[Expression]
```

#### 1.2 Workflow Parser (`src/parsers/workflow_parser.py`)

**Responsibilities:**
- Parse SSIS package Control Flow
- Extract task definitions and execution order
- Handle workflow variables and parameters
- Map workflow events and error handling

**Key Classes:**
```python
class WorkflowParser:
    """Parser for SSIS package files."""
    def parse(self, xml_path: str) -> Workflow
    def extract_tasks(self, root: Element) -> List[Task]
    def extract_links(self, root: Element) -> List[TaskLink]
    def build_execution_order(self, tasks: List[Task], links: List[TaskLink]) -> List[Task]

class Workflow:
    """Represents a complete SSIS package."""
    name: str
    folder: str
    tasks: List[Task]
    links: List[TaskLink]
    variables: List[WorkflowVariable]
    events: List[Event]
    execution_order: List[Task]

class Task:
    """Base class for workflow tasks."""
    name: str
    type: TaskType  # START, SESSION, COMMAND, DECISION, etc.
    attributes: Dict[str, Any]

class SessionTask(Task):
    """Represents a session task in a workflow."""
    mapping_name: str
    session_config: SessionConfig
    parameter_file: Optional[str]
```

#### 1.3 Session Parser (`src/parsers/session_parser.py`)

**Responsibilities:**
- Parse session configuration from workflow
- Extract connection assignments
- Handle partitioning strategies
- Process pre/post session commands

**Key Classes:**
```python
class SessionParser:
    """Parser for SSIS package configurations."""
    def parse(self, session_element: Element) -> SessionConfig
    def extract_connections(self, element: Element) -> Dict[str, Connection]
    def extract_parameters(self, element: Element) -> Dict[str, str]
    def extract_partitioning(self, element: Element) -> PartitionStrategy

class SessionConfig:
    """Represents session-level configuration."""
    name: str
    mapping_name: str
    connections: Dict[str, Connection]
    parameters: Dict[str, str]
    partition_strategy: PartitionStrategy
    pre_session_command: Optional[str]
    post_session_command: Optional[str]
    error_handling: ErrorHandling
    commit_interval: int
    buffer_size: int

class PartitionStrategy:
    """Represents session partitioning configuration."""
    type: PartitionType  # PASS_THROUGH, ROUND_ROBIN, HASH, KEY_RANGE
    partition_points: List[str]
    num_partitions: int
```

---

### 2. Domain Knowledge Layer

**Location:** `knowledge/transformations/`

Each transformation type has a dedicated knowledge file containing:
- Transformation semantics and behavior
- Input/output port characteristics
- Property configurations
- PySpark conversion patterns
- Edge cases and limitations
- Example conversions

See `knowledge/transformations/README.md` for the complete list.

---

### 3. LLM Provider Layer (`src/llm/`)

**Responsibilities:**
- Abstract LLM interactions across providers
- Manage prompts with domain knowledge context
- Handle rate limiting and retries
- Parse and validate LLM responses

**Key Classes:**
```python
class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""
    @abstractmethod
    async def generate(self, prompt: str, context: Dict) -> str
    @abstractmethod
    async def generate_with_schema(self, prompt: str, schema: Dict) -> Dict

class AnthropicProvider(BaseLLMProvider):
    """Claude integration for Anthropic API."""
    model: str = "claude-3-opus-20240229"

class GeminiProvider(BaseLLMProvider):
    """Google Gemini integration."""
    model: str = "gemini-pro"

class OllamaProvider(BaseLLMProvider):
    """Local model integration via Ollama."""
    model: str = "codellama"

class PromptManager:
    """Manages prompt construction with domain knowledge."""
    def build_transformation_prompt(self, transformation: Transformation) -> str
    def build_expression_prompt(self, expression: Expression) -> str
    def inject_domain_knowledge(self, prompt: str, transform_type: str) -> str
    def load_template(self, template_name: str) -> str

class LLMFactory:
    """Factory for creating LLM provider instances."""
    @staticmethod
    def create(provider_name: str, config: Dict) -> BaseLLMProvider
```

---

### 4. Generator Layer (`src/generators/`)

#### 4.1 PySpark Generator (`src/generators/pyspark_output.py`)

**Responsibilities:**
- Generate PySpark code from parsed structures
- Apply transformation-specific patterns
- Format and organize output code
- Handle imports and dependencies

**Key Functions:**
```python
class PySparkGenerator:
    """Generates PySpark code from parsed SSIS packages."""

    def generate(self, mapping: Mapping, session_config: SessionConfig) -> str
    def generate_source_read(self, source: Source, connection: Connection) -> str
    def generate_transformation(self, transform: Transformation) -> str
    def generate_target_write(self, target: Target, connection: Connection) -> str
    def assemble_pipeline(self, components: List[str]) -> str
    def format_code(self, code: str) -> str
```

#### 4.2 Workflow Generator (`src/generators/workflow_output.py`)

**Responsibilities:**
- Generate workflow orchestration code
- Support multiple orchestration targets (Airflow, Prefect)
- Handle task dependencies and execution order
- Convert workflow variables and parameters

**Key Classes:**
```python
class WorkflowGenerator:
    """Generates workflow orchestration code."""

    def generate_airflow_dag(self, workflow: Workflow) -> str
    def generate_prefect_flow(self, workflow: Workflow) -> str
    def generate_task(self, task: Task) -> str
    def generate_dependencies(self, links: List[TaskLink]) -> str

class AirflowDAGGenerator(WorkflowGenerator):
    """Generates Apache Airflow DAG files."""
    def generate(self, workflow: Workflow) -> str

class PrefectFlowGenerator(WorkflowGenerator):
    """Generates Prefect flow files."""
    def generate(self, workflow: Workflow) -> str
```

---

## Data Flow

### Mapping Conversion Flow

```
1. User provides SSIS DTSX file via CLI
         │
         ▼
2. XML Parser extracts mapping structure
   - Sources, Targets, Transformations
   - Build dependency graph
         │
         ▼
3. Load domain knowledge for each transformation type
   - Semantics and behavior
   - Conversion patterns
         │
         ▼
4. For each transformation in dependency order:
   a. Build context with domain knowledge
   b. Send to LLM with appropriate prompt
   c. Parse and validate response
   d. Store generated PySpark snippet
         │
         ▼
5. Assemble complete PySpark script
   - Import statements
   - SparkSession initialization
   - Source reads
   - Transformation chain
   - Target writes
         │
         ▼
6. Format and write output
```

### Workflow Conversion Flow

```
1. Parse workflow XML
         │
         ▼
2. Extract all tasks and links
   - Session tasks → Mapping conversions
   - Command tasks → Shell/Python tasks
   - Decision tasks → Branching logic
         │
         ▼
3. For each session task:
   a. Parse associated mapping
   b. Extract session configuration
   c. Generate PySpark script
         │
         ▼
4. Build execution order from links
         │
         ▼
5. Generate orchestration code
   - Task definitions
   - Dependencies
   - Error handling
   - Scheduling
         │
         ▼
6. Output workflow file (DAG/Flow)
```

---

## Transformation Mapping Strategy

### Standard Transformations

| SSIS Type | PySpark Equivalent | Notes |
|-----------------|-------------------|-------|
| Source Qualifier | `spark.read.*` | Based on connection type |
| Filter | `.filter()` / `.where()` | Direct expression conversion |
| Expression | `.withColumn()` / `.select()` | Port-by-port conversion |
| Aggregator | `.groupBy().agg()` | Handle sorted input option |
| Joiner | `.join()` | Support all join types |
| Lookup | `.join()` with broadcast | Handle connected/unconnected |
| Router | Multiple `.filter()` branches | One DF per group |
| Sorter | `.orderBy()` | Handle distinct option |
| Sequence Generator | `monotonically_increasing_id()` | Or window function |
| Normalizer | `.explode()` / flatten | Array handling |
| Union | `.union()` / `.unionByName()` | Schema alignment |
| Rank | `.withColumn()` + window | ROW_NUMBER/RANK |
| Update Strategy | Flag column | DD_INSERT/UPDATE/DELETE |

### Workflow Task Mapping

| SSIS Task | Airflow Equivalent | Prefect Equivalent |
|-----------------|-------------------|-------------------|
| Start | DAG start | Flow start |
| Session | PythonOperator/SparkSubmitOperator | @task |
| Command | BashOperator | ShellOperation |
| Decision | BranchPythonOperator | Conditional flow |
| Assignment | Variable setting | Context update |
| Timer | TimeSensor | Sleep/Schedule |
| Event Wait | ExternalTaskSensor | wait_for |
| Event Raise | TriggerDagRunOperator | run_deployment |

---

## Session Configuration Mapping

| SSIS Setting | PySpark/Spark Equivalent |
|--------------------|-------------------------|
| Commit Interval | Batch write size |
| Buffer Size | `spark.sql.shuffle.partitions` |
| DTM Buffer Size | `spark.executor.memory` |
| Partition Type | DataFrame repartitioning strategy |
| Error Handling | Try/except with logging |
| Pre-Session SQL | DataFrame SQL execution |
| Post-Session SQL | DataFrame SQL execution |

---

## Configuration

Configuration managed via `config/default.yaml`:

```yaml
llm:
  provider: anthropic  # anthropic, gemini, ollama
  model: claude-3-opus-20240229
  temperature: 0.1
  max_tokens: 4096

conversion:
  include_comments: true
  code_style: black
  spark_version: "3.5"

workflow:
  orchestrator: airflow  # airflow, prefect
  airflow_version: "2.7"

output:
  directory: output/
  create_subdirs: true
  generate_tests: false

logging:
  level: INFO
  file: logs/conversion.log
```

---

## Evaluation Framework

Located in `evals/`:
- Compare generated PySpark against golden outputs
- Measure transformation accuracy
- Generate conversion quality reports
- Track conversion confidence scores

```python
class ConversionEvaluator:
    def evaluate(self, generated: str, golden: str) -> EvaluationResult
    def compare_schemas(self, gen_schema: Schema, gold_schema: Schema) -> float
    def compare_data(self, gen_df: DataFrame, gold_df: DataFrame) -> float
    def generate_report(self, results: List[EvaluationResult]) -> Report
```

---

## Error Handling Strategy

1. **Parsing Errors**: Log and skip malformed elements, continue with valid parts
2. **Unsupported Features**: Mark for manual review, generate placeholder code
3. **LLM Errors**: Retry with exponential backoff, fallback to template-based generation
4. **Validation Errors**: Generate warnings in output, flag for review

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-02 | System | Added workflow and session design |
