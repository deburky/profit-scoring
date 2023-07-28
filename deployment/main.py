import time
import fetch_and_write_data
import dbt_transform_data
import run_scoring

def main():
    start_time = time.time()

    # Step 1: Fetch and Write Data
    fetch_and_write_data.run()

    # Step 2: Transform Data with dbt
    dbt_transform_data.run()

    # Step 3: Run Scoring Model
    run_scoring.run()

    print(f"Entire process completed successfully \u2713")
    print(f"Total Time: {time.time() - start_time:.3f}s")

if __name__ == "__main__":
    main()
