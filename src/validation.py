from datetime import UTC, datetime
from typing import Any, Dict, List, Tuple

from frictionless import Resource, Schema, fields
from uuid6 import uuid7

validation_schema = Schema(
    fields=[
        fields.AnyField(name="id", constraints={"required": True}),
        fields.StringField(name="hash", constraints={"required": True, "unique": True}),
        fields.StringField(name="store_name", constraints={"required": True}),
        fields.StringField(name="item_code", constraints={"required": True}),
        fields.StringField(name="item_barcode", constraints={"required": True}),
        fields.StringField(name="description", constraints={"required": True}),
        fields.StringField(name="category", constraints={"required": True}),
        fields.StringField(name="department", constraints={"required": True}),
        fields.StringField(name="sub_department", constraints={"required": True}),
        fields.StringField(name="section", constraints={"required": True}),
        fields.NumberField(
            name="quantity", constraints={"minimum": 0.0, "required": True}
        ),
        fields.NumberField(
            name="total_sales", constraints={"minimum": 0.0, "required": True}
        ),
        fields.NumberField(name="rrp", constraints={"minimum": 0.0, "required": True}),
        fields.StringField(name="supplier", constraints={"required": True}),
        fields.DateField(
            name="date_of_sale",
            constraints={"required": True, "maximum": datetime.now(UTC).date},
        ),
    ],
    primary_key=["id"],
)


def data_validate(data: List[Dict]) -> Tuple[List[str], List[Any]]:
    errors_to_insert = []
    ids = []

    if data:
        with Resource(data=data, schema=validation_schema) as resource:
            report = resource.validate()

        if report:
            for error in report.flatten(
                [
                    "cell",
                    "rowNumber",
                    "fieldName",
                    "fieldNumber",
                    "message",
                    "cells",
                    "note",
                ]
            ):
                row_loc = error[5][0]
                row_number = str(error[1])
                field_name = error[2]
                field_number = str(error[3])
                message = (
                    error[4]
                    .replace(field_number, field_name)
                    .replace(row_number, row_loc)
                )

                error_record = {
                    "id": uuid7(),
                    "row_id": row_loc,
                    "hash": error[5][1],
                    "field_name": field_name,
                    "cell_value": error[0],
                    "message": message,
                    "note": error[6],
                }

                errors_to_insert.append(error_record)
                ids.append(row_loc)

    return (ids, errors_to_insert)
