from setuptools import setup, find_packages

setup(
    name="pardox-server",
    version="0.3.4",
    description="pardoX Server — PostgreSQL wire protocol server backed by pardox_cpu",
    packages=find_packages(exclude=["__pycache__"]),
    python_requires=">=3.9",
    install_requires=["flask>=2.0"],
    entry_points={
        "console_scripts": [
            "pardox-server=pardox_server.__main__:main",
        ],
    },
)
