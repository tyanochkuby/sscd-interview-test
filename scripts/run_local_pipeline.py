import os
import shutil
import subprocess
import sys

from src.interview_task.config import OUTPUT_PATH, PROJECT_ROOT


MAGE_PROJECT_PATH = PROJECT_ROOT / "mage_project"
PIPELINE_UUID = "order_events_pipeline"


def main() -> None:
    env = os.environ.copy()
    env.setdefault("RUN_PIPELINE_IN_ONE_PROCESS", "True")

    if OUTPUT_PATH.exists():
        shutil.rmtree(OUTPUT_PATH)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "mage_ai.cli.main",
            "run",
            str(MAGE_PROJECT_PATH),
            PIPELINE_UUID,
        ],
        check=True,
        env=env,
    )


if __name__ == "__main__":
    main()
