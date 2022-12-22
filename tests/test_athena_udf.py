import base64
import copy
import datetime
import uuid
from unittest.mock import ANY, Mock
import pyarrow as pa

from athena_udf import BaseAthenaUDF

REQUEST_TEMPLATE = {
    "@type": "UserDefinedFunctionRequest",
    "identity": {
        "arn": "UNKNOWN_ARN",
        "account": "12345",
        "principalTags": {},
        "iamGroups": [],
    },
    "inputRecords": {
        "aId": "9af9372c-1d73-49ed-9e26-c47fc7c91615",
        "schema": "sss",
        "records": "sss",
    },
    "outputSchema": {"schema": "sss"},
    "methodName": "my_udf",
    "functionType": "SCALAR",
}


class TestSimpleVarcharUDF(BaseAthenaUDF):
    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        return arguments[0].upper()


class TestTwoVarcharUDF(BaseAthenaUDF):
    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        return arguments[0] + arguments[1]


class TestArrayUDF(BaseAthenaUDF):
    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        return [arguments[0]] + arguments[1]


class TestDateUDF(BaseAthenaUDF):
    @staticmethod
    def handle_athena_record(input_schema, output_schema, arguments):
        return arguments[0]


def test_ping_request():
    ping_request = {
        "@type": "PingRequest",
        "identity": {
            "id": "UNKNOWN",
            "principal": "UNKNOWN",
            "account": "12345",
            "arn": "UNKNOWN_ARN",
            "tags": {},
            "groups": [],
        },
        "catalogName": "my_udf",
        "queryId": "6c59fb04-9bb4-48ea-9afb-971fe1edd2c2",
    }

    test_udf = TestSimpleVarcharUDF()
    test_udf.handle_athena_record = Mock()
    resp = test_udf.lambda_handler(ping_request, None)
    assert resp == {
        "@type": "PingResponse",
        "catalogName": "event",
        "queryId": ANY,
        "sourceType": "athena_udf",
        "capabilities": 23,
    }
    test_udf.handle_athena_record.assert_not_called()


def test_simple_varchar():
    request = copy.copy(REQUEST_TEMPLATE)
    schema = pa.schema([("0", pa.string())])
    request["inputRecords"]["schema"] = base64.b64encode(schema.serialize())
    request["outputSchema"]["schema"] = base64.b64encode(schema.serialize())

    inputs = ["foo", "bar"]
    records = base64.b64encode(
        pa.RecordBatch.from_arrays([inputs], schema=schema).serialize()
    ).decode()
    request["inputRecords"]["records"] = records
    test_udf = TestSimpleVarcharUDF()
    test_udf.handle_ping = Mock()
    resp = test_udf.lambda_handler(request, None)
    test_udf.handle_ping.assert_not_called()
    uuid.UUID(resp["records"]["aId"])

    assert resp["methodName"] == "my_udf"
    assert resp["@type"] == "UserDefinedFunctionResponse"
    output_schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(resp["records"]["schema"]))
    )
    record_batch = pa.ipc.read_record_batch(
        pa.BufferReader(base64.b64decode(resp["records"]["records"])),
        output_schema,
    )
    record_batch_list = [x["0"] for x in record_batch.to_pylist()]
    assert record_batch_list == ["FOO", "BAR"]


def test_two_varchars():
    request = copy.copy(REQUEST_TEMPLATE)
    input_schema = pa.schema(
        [
            ("0", pa.string()),
            ("1", pa.string()),
        ]
    )
    output_schema = pa.schema([("0", pa.string())])

    request["inputRecords"]["schema"] = base64.b64encode(
        input_schema.serialize()
    )
    request["outputSchema"]["schema"] = base64.b64encode(
        output_schema.serialize()
    )

    inputs = [["foo", "fud"], ["bar", "bud"]]
    records = base64.b64encode(
        pa.RecordBatch.from_arrays(inputs, schema=input_schema).serialize()
    ).decode()
    request["inputRecords"]["records"] = records
    test_udf = TestTwoVarcharUDF()
    test_udf.handle_ping = Mock()
    resp = test_udf.lambda_handler(request, None)
    test_udf.handle_ping.assert_not_called()
    uuid.UUID(resp["records"]["aId"])

    assert resp["methodName"] == "my_udf"
    assert resp["@type"] == "UserDefinedFunctionResponse"
    output_schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(resp["records"]["schema"]))
    )
    record_batch = pa.ipc.read_record_batch(
        pa.BufferReader(base64.b64decode(resp["records"]["records"])),
        output_schema,
    )
    record_batch_list = [x["0"] for x in record_batch.to_pylist()]
    assert record_batch_list == ["foobar", "fudbud"]


def test_array():
    request = copy.copy(REQUEST_TEMPLATE)
    incoming_schema = pa.schema(
        [("0", pa.string()), ("1", pa.list_(pa.string()))]
    )

    output_schema = pa.schema([("0", pa.list_(pa.string()))])
    request["inputRecords"]["schema"] = base64.b64encode(
        incoming_schema.serialize()
    )
    request["outputSchema"]["schema"] = base64.b64encode(
        output_schema.serialize()
    )

    inputs = [["foo1", "foo2"], [["boo1", "boo2"], ["baa1", "baa2"]]]
    records = base64.b64encode(
        pa.RecordBatch.from_arrays(inputs, schema=incoming_schema).serialize()
    ).decode()
    request["inputRecords"]["records"] = records
    test_udf = TestArrayUDF()
    test_udf.handle_ping = Mock()
    resp = test_udf.lambda_handler(request, None)
    test_udf.handle_ping.assert_not_called()
    uuid.UUID(resp["records"]["aId"])

    assert resp["methodName"] == "my_udf"
    assert resp["@type"] == "UserDefinedFunctionResponse"
    output_schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(resp["records"]["schema"]))
    )
    record_batch = pa.ipc.read_record_batch(
        pa.BufferReader(base64.b64decode(resp["records"]["records"])),
        output_schema,
    )
    record_batch_list = [x["0"] for x in record_batch.to_pylist()]
    assert record_batch_list == [["foo1", "boo1", "boo2"], ["foo2", "baa1", "baa2"]]


def test_datetime():
    request = copy.copy(REQUEST_TEMPLATE)
    schema = pa.schema([("0", pa.date64())])
    request["inputRecords"]["schema"] = base64.b64encode(schema.serialize())
    request["outputSchema"]["schema"] = base64.b64encode(schema.serialize())
    date1 = datetime.datetime.now().date()
    date2 = datetime.datetime.now().date()
    inputs = [date1, date2]
    records = base64.b64encode(
        pa.RecordBatch.from_arrays([inputs], schema=schema).serialize()
    ).decode()
    request["inputRecords"]["records"] = records
    test_udf = TestDateUDF()
    test_udf.handle_ping = Mock()
    resp = test_udf.lambda_handler(request, None)
    test_udf.handle_ping.assert_not_called()
    uuid.UUID(resp["records"]["aId"])

    assert resp["methodName"] == "my_udf"
    assert resp["@type"] == "UserDefinedFunctionResponse"
    output_schema = pa.ipc.read_schema(
        pa.BufferReader(base64.b64decode(resp["records"]["schema"]))
    )
    record_batch = pa.ipc.read_record_batch(
        pa.BufferReader(base64.b64decode(resp["records"]["records"])),
        output_schema,
    )
    record_batch_list = [x["0"] for x in record_batch.to_pylist()]
    assert record_batch_list == [date1, date2]
