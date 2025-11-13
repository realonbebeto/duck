import hashlib
import io
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import ORJSONResponse
from uuid6 import uuid7

from src.db import DuckDB
from src.schemas import (
    DataIngestionResponse,
    DataQualityReport,
    PricingReport,
    PromotionReport,
    ValidationReport,
)
from src.services import (
    pricing_report,
    promotion_report,
    quality_report,
    validation_report,
)

app = FastAPI(title="Retail Analytics Platform", openapi_url="/v1/openapi.json")
db = DuckDB()


@app.get("/")
def root():
    return {"message": "Retail Analytics API", "version": "1.0"}


@app.post("/api/ingest", response_model=DataIngestionResponse)
async def ingest(file: UploadFile = File(...)):
    """
    Ingest sales data from CSV file into DuckDB.
    """

    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents), parse_dates=["Date Of Sale"])

        columns = [
            col.lower().strip().replace(" ", "_").replace("-", "_")
            for col in df.columns
        ]

        df.columns = columns
        df["hash"] = df.apply(
            lambda row: hashlib.sha256(
                str(tuple(row.values)).encode("utf-8")
            ).hexdigest(),
            axis=1,
        )
        df["id"] = df["store_name"].apply(lambda x: uuid7().hex)

        df = df[
            [
                "id",
                "hash",
                "store_name",
                "item_code",
                "item_barcode",
                "supplier",
                "description",
                "category",
                "department",
                "sub_department",
                "section",
                "quantity",
                "total_sales",
                "rrp",
                "date_of_sale",
            ]
        ]

        db.conn.execute("INSERT INTO duck_store_staging SELECT * FROM df;")

        ids = db.process_validation()

        if ids:
            # Store proper records to duck_store
            db.conn.execute(
                f"""INSERT INTO duck_store 
                        SELECT 
                        id,
                        hash,
                        store_name,
                        item_code,
                        item_barcode,
                        supplier,
                        description,
                        category,
                        department,
                        sub_department,
                        section,
                        quantity,
                        total_sales,
                        rrp,
                        total_sales/quantity AS sale_price,
                        total_sales/quantity - rrp AS margin,
                        date_of_sale
                      FROM duck_store_staging WHERE id NOT IN ({",".join(f"'{s}'" for s in ids)});"""
            )

        return ORJSONResponse(
            content=jsonable_encoder(
                DataIngestionResponse(
                    rows_for_review=len(ids),
                    rows_ingested=df.shape[0] - len(ids),
                    rows_received=df.shape[0],
                    message="Successful ingestion",
                )
            )
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@app.get("/api/errors", response_model=ValidationReport)
def validation_errors():
    try:
        report = validation_report()
        return ORJSONResponse(content=jsonable_encoder(report))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Validation errors read failed: {str(e)}"
        )


@app.get("/api/quality", response_model=DataQualityReport)
def data_quality():
    try:
        report = quality_report()

        return ORJSONResponse(content=jsonable_encoder(report))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Quality check failed: {str(e)}")


@app.get("/api/promo", response_model=PromotionReport)
def sales_promotion(supplier: str, min_uplift: Optional[float] = None):
    try:
        report = promotion_report(supplier, min_uplift)

        return ORJSONResponse(content=jsonable_encoder(report))

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Promotion analysis failed: {str(e)}"
        )


@app.get("/api/pricing", response_model=PricingReport)
def pricing_index(supplier: str):
    try:
        report = pricing_report(supplier)

        return ORJSONResponse(content=jsonable_encoder(report))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Pricing analysis failed: {str(e)}"
        )
