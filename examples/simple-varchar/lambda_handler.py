import athena_udf


class SimpleVarcharUDF(athena_udf.BaseAthenaUDF):

    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        varchar = arguments[0]
        return varchar.lower()


lambda_handler = SimpleVarcharUDF().lambda_handler
