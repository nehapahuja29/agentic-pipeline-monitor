import random
from datetime import datetime, timedelta

def generate_pipeline_run(inject_failure=None):
    """
    Generates a fake pipeline run record.
    inject_failure options: 'row_count', 'null_rate', 'latency', 'schema', None
    """
    base_row_count = 10000
    base_null_rate = 0.02  # 2% nulls is normal
    base_latency = 45      # seconds, normal run time

    run = {
        "pipeline_name": "transactions_daily",
        "run_id": f"run_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "timestamp": datetime.now().isoformat(),
        "status": "completed",
        "rows_processed": base_row_count + random.randint(-500, 500),
        "null_rate": round(random.uniform(0.01, 0.03), 4),
        "latency_seconds": base_latency + random.randint(-5, 10),
        "schema_valid": True,
        "expected_row_count": base_row_count,
        "source": "transactions_db",
        "destination": "analytics_warehouse"
    }

    # Inject specific failure if requested
    if inject_failure == "row_count":
        run["rows_processed"] = random.randint(100, 500)  # way too low
        run["status"] = "completed_with_warnings"

    elif inject_failure == "null_rate":
        run["null_rate"] = round(random.uniform(0.30, 0.60), 4)  # 30-60% nulls, very bad
        run["status"] = "completed_with_warnings"

    elif inject_failure == "latency":
        run["latency_seconds"] = random.randint(300, 600)  # 5-10 mins, way too slow
        run["status"] = "completed_with_warnings"

    elif inject_failure == "schema":
        run["schema_valid"] = False
        run["status"] = "failed"
        run["error"] = "Column 'transaction_amount' missing from source"

    return run


def generate_batch(num_healthy=5, failure_type=None):
    """
    Generates a batch of pipeline runs — healthy ones plus one failure.
    """
    runs = []

    # Generate healthy runs
    for _ in range(num_healthy):
        runs.append(generate_pipeline_run(inject_failure=None))

    # Add one failure at the end
    if failure_type:
        runs.append(generate_pipeline_run(inject_failure=failure_type))

    return runs


if __name__ == "__main__":
    import json

    print("=== Healthy Pipeline Run ===")
    healthy = generate_pipeline_run()
    print(json.dumps(healthy, indent=2))

    print("\n=== Failed Pipeline Run (row count drop) ===")
    failed = generate_pipeline_run(inject_failure="row_count")
    print(json.dumps(failed, indent=2))

    print("\n=== Failed Pipeline Run (schema error) ===")
    schema_fail = generate_pipeline_run(inject_failure="schema")
    print(json.dumps(schema_fail, indent=2))