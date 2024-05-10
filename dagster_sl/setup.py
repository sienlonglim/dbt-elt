from setuptools import find_packages, setup

setup(
    name="dagster_orchestration",
    packages=find_packages(exclude=["dagster_orchestration_tests"]),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dagster_duckdb_pandas",
        "dagster_duckdb",
        "dagster-cloud",
        "Faker==18.4.0",
        "matplotlib",
        "pandas",
        "requests",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
