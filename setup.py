from setuptools import setup, find_packages

setup(
    name="data-sdk",
    version="0.7.4",
    packages=["data_sdk", "data_sdk.crawlers", "data_sdk.wrappers"],
    package_dir={"data_sdk": "src"},
    install_requires=[
        "numba",
        "numpy",
        "pandas",
        "polars",
        "FinMind",
        "shioaji",
        "requests",
        "tejapi",
        # data_sdk.crawlers.warrant
        "dlt",
        "beautifulsoup4",
        "lxml",
        "pyarrow",
    ],
    author="User",
    description="Data SDK for FinMind and Shioaji wrappers and crawlers",
    python_requires=">=3.6",
)
