#!/usr/bin/env python3

import subprocess
import sys
import os


def run_step(name, command, cwd=None, shell=False):
    print(f"Running: {name}")
    try:
        subprocess.run(command, check=True, cwd=cwd, shell=shell)
        print(f"Completed: {name}\n")
    except subprocess.CalledProcessError as e:
        print(f"Failed during: {name}")
        sys.exit(1)


def main():
    # install requirements from file
    if os.path.exists("requirements.txt"):
        run_step(
            "Installing requirements",
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
        )
    else:
        print("requirements.txt not found. Skipping dependency installation.\n")

    # ETL step to clean and load messages into SQLite
    run_step(
        "ETL - Raw data ingestion.py",
        [sys.executable, "src/ingest_raw_data.py"],
    )

    # Trusted Tables
    run_step(
        "Modeled layer - Trusted Tables",
        [sys.executable, "src/trusted_tables.py"],
    )

    # Trusted Tables
    run_step(
        "Modeled layer - Refined Tables",
        [sys.executable, "src/refined_tables.py"],
    )

    print("Pipeline execution finished.")


if __name__ == "__main__":
    main()