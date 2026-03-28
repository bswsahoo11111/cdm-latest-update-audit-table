from setuptools import setup, find_packages

setup(
    name="datamart_gap_upsert_pipeline",       # package name
    version="0.11.0",                  # version
    packages=find_packages(),         # automatically find dev/, prod/, etc.
    install_requires=[
        "pyspark",                    # dependencies
    ],
    entry_points={
        "console_scripts": [
            "gap-upsert=main:main",   # optional CLI entry point
        ],
    },
)