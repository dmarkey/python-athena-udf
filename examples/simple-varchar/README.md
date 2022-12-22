# Simple Varchar example

This example simply takes a string and returns the lowercase version.
## Building

Build the ARM64 lambda ready to deploy using `make build`

This lambda is over the GUI upload limit in the console so you will have to use an S3 bucket.

## Example usage

Assuming the lambda function is called `my-lambda` then run a query like this:
```sql
USING EXTERNAL FUNCTION my_udf(col1 varchar) RETURNS varchar LAMBDA 'athena-test'

SELECT my_udf('FooBar');
```

Which will yield the result `foobar`