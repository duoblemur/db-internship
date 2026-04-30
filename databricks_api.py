# databricks_api.py

import requests
import time
from config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN
)

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}


def create_cluster():
    """
    Creates a new cluster using REST API
    API:
    POST /api/2.0/clusters/create
    """

    url = f"{DATABRICKS_HOST}/api/2.0/clusters/create"

    payload = {
        "cluster_name": "liebiedieva-api-cluster",
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
        "autotermination_minutes": 30,
        "data_security_mode": "SINGLE_USER"
    }

    response = requests.post(
        url,
        headers=HEADERS,
        json=payload
    )

    response.raise_for_status()

    result = response.json()
    cluster_id = result["cluster_id"]

    print(f"Cluster created successfully, cluster ID: {cluster_id}")

    return cluster_id


def wait_for_cluster(cluster_id):
    """
    Wait until cluster becomes RUNNING
    API:
    GET /api/2.0/clusters/get
    """

    url = f"{DATABRICKS_HOST}/api/2.0/clusters/get"

    while True:
        response = requests.get(
            url,
            headers=HEADERS,
            params={"cluster_id": cluster_id}
        )

        response.raise_for_status()

        result = response.json()
        state = result["state"]

        print(f"Cluster state: {state}")

        if state == "RUNNING":
            print("Cluster is ready")
            return

        if state in ["TERMINATED", "TERMINATING", "ERROR", "UNKNOWN"]:
            raise Exception(f"Cluster failed with state: {state}")

        time.sleep(20)


def run_job(job_id):
    """
    Run an existing Databricks Job by job_id
    API:
    POST /api/2.2/jobs/run-now
    """

    url = f"{DATABRICKS_HOST}/api/2.2/jobs/run-now"

    payload = {
        "job_id": job_id
    }

    response = requests.post(
        url,
        headers=HEADERS,
        json=payload
    )

    response.raise_for_status()

    result = response.json()
    run_id = result["run_id"]

    print(f"Job triggered successfully, run ID: {run_id}")

    return run_id


def submit_notebook_run(existing_cluster_id, notebook_path):
    """
    Run a notebook directly on a specific cluster
    API:
    POST /api/2.1/jobs/runs/submit
    """

    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"

    payload = {
        "run_name": "API-triggered-notebook-run",
        "existing_cluster_id": existing_cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }

    response = requests.post(
        url,
        headers=HEADERS,
        json=payload
    )

    response.raise_for_status()

    result = response.json()
    run_id = result["run_id"]

    print(f"Notebook submitted successfull, run ID: {run_id}")

    return run_id


def monitor_run(run_id):
    """
    Monitor job execution status
    API:
    GET /api/2.2/jobs/runs/get
    """

    url = f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get"

    while True:
        response = requests.get(
            url,
            headers=HEADERS,
            params={"run_id": run_id}
        )

        response.raise_for_status()

        result = response.json()

        life_cycle_state = result["state"]["life_cycle_state"]
        result_state = result["state"].get(
            "result_state",
            "PENDING"
        )

        print(
            f"Run status: "
            f"{life_cycle_state} / {result_state}"
        )

        if life_cycle_state in [
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR"
        ]:
            print("Job finished.")
            return result_state

        time.sleep(15)


def trigger_pipeline(pipeline_id, full_refresh=False):
    """
    Trigger a specific DLT pipeline
    API:
    POST /api/2.0/pipelines/{pipeline_id}/updates
    """

    url = (
        f"{DATABRICKS_HOST}"
        f"/api/2.0/pipelines/{pipeline_id}/updates"
    )

    payload = {
        "full_refresh": full_refresh
    }

    response = requests.post(
        url,
        headers=HEADERS,
        json=payload
    )

    response.raise_for_status()

    result = response.json()

    print(
        f"Pipeline {pipeline_id} triggered successfully"
    )
    print(result)

    return result