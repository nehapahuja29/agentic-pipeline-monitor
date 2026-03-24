from langchain_aws import ChatBedrock
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import json

load_dotenv()

# ── Output schema — forces the agent to return structured data ──
class MonitorOutput(BaseModel):
    is_healthy: bool = Field(description="True if pipeline is healthy, False if anomaly detected")
    failure_type: str = Field(description="One of: row_count, null_rate, latency, schema, none")
    severity: str = Field(description="One of: low, medium, high, critical")
    summary: str = Field(description="One sentence explaining what was detected")
    recommended_action: str = Field(description="What should happen next: investigate, remediate, escalate")

# ── LLM setup ──
llm = ChatBedrock(
    model_id="us.anthropic.claude-3-5-haiku-20241022-v1:0",
    region_name="us-east-1"
)

# ── Structured output ──
structured_llm = llm.with_structured_output(MonitorOutput)

# ── Prompt ──
prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a data pipeline monitoring agent. Your job is to analyze 
pipeline run metrics and detect anomalies.

Use these thresholds to evaluate health:
- Row count: flag if rows_processed is less than 50% of expected_row_count
- Null rate: flag if null_rate exceeds 0.10 (10%)
- Latency: flag if latency_seconds exceeds 120 seconds
- Schema: flag if schema_valid is False

Be precise and consistent. Always return structured output."""),
    ("human", """Analyze this pipeline run and detect any anomalies:

{pipeline_run}

Return your assessment in the required format.""")
])

# ── Chain ──
monitor_chain = prompt | structured_llm

def run_monitor_agent(pipeline_run: dict) -> MonitorOutput:
    """Takes a pipeline run dict and returns a structured health assessment."""
    result = monitor_chain.invoke({
        "pipeline_run": json.dumps(pipeline_run, indent=2)
    })
    return result


if __name__ == "__main__":
    from data.synthetic_data import generate_pipeline_run

    print("=== Testing Monitor Agent ===\n")

    # Test 1: healthy run
    print("--- Healthy Run ---")
    healthy = generate_pipeline_run()
    result = run_monitor_agent(healthy)
    print(f"Healthy: {result.is_healthy}")
    print(f"Failure type: {result.failure_type}")
    print(f"Severity: {result.severity}")
    print(f"Summary: {result.summary}")
    print(f"Action: {result.recommended_action}")

    print("\n--- Row Count Failure ---")
    failed = generate_pipeline_run(inject_failure="row_count")
    result = run_monitor_agent(failed)
    print(f"Healthy: {result.is_healthy}")
    print(f"Failure type: {result.failure_type}")
    print(f"Severity: {result.severity}")
    print(f"Summary: {result.summary}")
    print(f"Action: {result.recommended_action}")

    print("\n--- Schema Failure ---")
    schema_fail = generate_pipeline_run(inject_failure="schema")
    result = run_monitor_agent(schema_fail)
    print(f"Healthy: {result.is_healthy}")
    print(f"Failure type: {result.failure_type}")
    print(f"Severity: {result.severity}")
    print(f"Summary: {result.summary}")
    print(f"Action: {result.recommended_action}")