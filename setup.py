from setuptools import setup, find_packages

setup(
    name="data-sdk",
    version="0.7.1",
    packages=["data_sdk", "data_sdk.crawlers", "data_sdk.wrappers"],
    package_dir={"data_sdk": "src"},
    install_requires=[
        "numba",
        "numpy",
        "pandas",
        "polars",
        "pyarrow",
        "FinMind",
        "finlab",
        "shioaji",
        "requests",
        "tejapi",
    ],
    author="User",
    description="Data SDK for FinMind and Shioaji wrappers and crawlers",
    python_requires=">=3.6",
)
