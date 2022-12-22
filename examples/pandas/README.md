# Pandas Example

This example takes an array of ints and returns an array of floats with the percentage change between elements using the pandas [pct_change](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.pct_change.html#pandas-dataframe-pct-change) function.

## Building

Build the ARM64 lambda ready to deploy using `make build`

This lambda is over the GUI upload limit in the console so you will have to use an S3 bucket.

## Example usage

Assuming the lambda function is called `my-lambda` then run a query like this:


```sql
USING EXTERNAL FUNCTION my_udf(col1 ARRAY(int)) RETURNS ARRAY(real) LAMBDA 'athena-test'

SELECT my_udf(ARRAY[1,2,3,4,5]);
```

And that will return

```sql
[NaN, 1.0, 0.5, 0.33333334, 0.25]
```