import os
import subprocess

from pyspark.sql import SparkSession


def _discover_java_home() -> str | None:
    current_java_home = os.environ.get("JAVA_HOME")
    if current_java_home:
        return current_java_home

    for version in ("21", "17"):
        try:
            return subprocess.check_output(
                ["/usr/libexec/java_home", "-v", version],
                text=True,
            ).strip()
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue

    return None


def build_local_spark(app_name: str) -> SparkSession:
    # Spark 4.1.1 supports JDK 17/21. Prefer a supported local runtime automatically.
    java_home = _discover_java_home()
    if java_home:
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ['PATH']}"

    # Spark can fall back to Hadoop user lookup, which breaks on some newer JDKs.
    os.environ.setdefault("SPARK_USER", "interview")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
