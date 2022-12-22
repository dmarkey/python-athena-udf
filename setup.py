from setuptools import setup
import os


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="athena-udf",
    description="Athena User Defined Functions(UDFs) made easy!",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="David Markey",
    url="https://github.com/dmarkey/python-athena-udf",
    project_urls={
        "Issues": "https://github.com/dmarkey/python-athena-udf/issues",
        "CI": "https://github.com/dmarkey/python-athena-udf/actions",
        "Changelog": "https://github.com/dmarkey/python-athena-udf/releases",
    },
    license="Apache License, Version 2.0",
    packages=["athena_udf"],
    install_requires=["pyarrow>10.0.0"],
    extras_require={"test": ["pytest"]},
    python_requires=">=3.7",
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
)
