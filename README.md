# athena-udf

[![PyPI](https://img.shields.io/pypi/v/athena-udf.svg)](https://pypi.org/project/athena-udf/)
[![Changelog](https://img.shields.io/github/v/release/dmarkey/python-athena-udf?include_prereleases&label=changelog)](https://github.com/dmarkey/athena-udf/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/dmarkey/python-athena-udf/blob/main/LICENSE)

Athena User Defined Functions(UDFs) in Python made easy!

This library implements the Athena UDF protocol in Python so you don't have to use Java and you can use any Python library you wish including numpy/pandas!

## Installation

Install this library using `pip`:

    pip install athena-udf

## Usage

Simply install the package, create a lambda handler Python file, subclass `BaseAthenaUDF` and implement the `handle_athena_record` static method with your required functionality like this:

```
import athena_udf


class SimpleVarcharUDF(athena_udf.BaseAthenaUDF):

    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        varchar = arguments[0]
        return varchar.lower()


lambda_handler = SimpleVarcharUDF().lambda_handler
```

This very basic example takes a `varchar` input, and returns the lowercase version.

`varchar` is converted to a python string on the way in and way out.

`input_schema` contains a `PyArrow` schema representing the schema of the data being passed

`output_schema` contains a `PyArrow` schema representing the schema of what athena expects to be returned.

`arguments` contains a list of arguments given to the function. Can be more than 1 with different types.

If you package the above into a zip, with dependencies and name your lambda function `my-kambda` you can then run it from the athena console like so:

```sql
USING EXTERNAL FUNCTION my_udf(col1 varchar) RETURNS varchar LAMBDA 'athena-test'

SELECT my_udf('FooBar');
```

Which will yield the result `foobar`

See other examples in the [examples](examples) folder of this repo.

## Important information before using.

Each lambda instance will take multiple requests for the same query. Each request can contain multiple rows, `athena-udf` handles this for you and your implementation will receive a single row.

Athena will group your data into around 1MB chunks in a single request. The maximum your function can return in 6MB per chunk. 

This library uses `PyArrow`. This is a large library so the Lambdas will be around 50MB zipped.

Timestamps seem to be truncated into Python `date` objects missing the time. 

Functions can return one value only. To return more complex data structures consider returning a JSON payload and parsing on athena.
## Development

To contribute to this library, first checkout the code. Then create a new virtual environment:

    cd athena-udf
    python -m venv venv
    source venv/bin/activate

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
