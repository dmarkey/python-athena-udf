from uuid import uuid4
import base64
import pyarrow as pa


class BaseAthenaUDF:
    @staticmethod
    def handle_ping(event):
        return {
            "@type": "PingResponse",
            "catalogName": "event",
            "queryId": event["queryId"],
            "sourceType": "athena_udf",
            "capabilities": 23,
        }

    def lambda_handler(self, event, context):
        incoming_type = event["@type"]
        if incoming_type == "PingRequest":
            return self.handle_ping(event)
        elif incoming_type == "UserDefinedFunctionRequest":
            return self.handle_udf_request(event)

        raise Exception(f"Unknown event type {incoming_type} from Athena")

    @classmethod
    def handle_udf_request(cls, event):
        input_schema = pa.ipc.read_schema(
            pa.BufferReader(base64.b64decode(event["inputRecords"]["schema"]))
        )
        output_schema = pa.ipc.read_schema(
            pa.BufferReader(base64.b64decode(event["outputSchema"]["schema"]))
        )
        record_batch = pa.ipc.read_record_batch(
            pa.BufferReader(
                base64.b64decode(event["inputRecords"]["records"])
            ),
            input_schema,
        )
        record_batch_list = record_batch.to_pylist()

        outputs = []
        for record in record_batch_list:
            output = cls.handle_athena_record(
                input_schema, output_schema, list(record.values())
            )
            outputs.append(output)
        return {
            "@type": "UserDefinedFunctionResponse",
            "records": {
                "aId": str(uuid4()),
                "schema": event["outputSchema"]["schema"],
                "records": base64.b64encode(
                    pa.RecordBatch.from_arrays(
                        [outputs], schema=output_schema
                    ).serialize()
                ).decode(),
            },
            "methodName": event["methodName"],
        }

    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        raise NotImplementedError()
