# main.py

from databricks_api import (
    create_cluster,
    wait_for_cluster,
    run_job,
    submit_notebook_run,
    monitor_run,
    trigger_pipeline
)

# functions to submit a single notebook run or to refresh a pipeline are also added, just were not run here

def main():
    print("Starting Databricks automation workflow...")

    JOB_ID = 223333185558640

    print("\nStep 1: Creating cluster via REST API")
    cluster_id = create_cluster()

    print("\nStep 2: Waiting for cluster to become RUNNING")
    wait_for_cluster(cluster_id)

    print("\nStep 3: Running existing Databricks Job")
    run_id = run_job(JOB_ID)

    print("\nStep 4: Monitoring job execution")
    final_status = monitor_run(run_id)

    print(f"\nFinal Job Status: {final_status}")

    print("\nAutomation workflow completed successfully!")


if __name__ == "__main__":
    main()