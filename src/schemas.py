import json
from typing import List

from pydantic import BaseModel


class DataIngestionResponse(BaseModel):
    rows_for_review: int
    rows_received: int
    rows_ingested: int
    message: str


class StoreQualityIssue(BaseModel):
    store_name: str
    issue_type: str
    details: dict


class SupplierQualityIssue(BaseModel):
    supplier: str
    issue_type: str
    details: dict


class DataQualityReport(BaseModel):
    unreliable_stores: List[StoreQualityIssue]
    unreliable_suppliers: List[SupplierQualityIssue]
    summary: dict


class PromoUplift(BaseModel):
    product_name: str
    section: str
    promo_units: float
    baseline_units: float
    uplift_pct: float
    total_promo_transactions: int


class PromotionReport(BaseModel):
    summary: dict
    top_performers: List[PromoUplift]
    poor_performers: List[PromoUplift]


class PriceIndex(BaseModel):
    store_name: str
    sub_department: str
    section: str
    supplier_avg_price: float
    competitor_avg_price: float
    price_index: float
    market_position: str


class PricingReport(BaseModel):
    supplier_pricing: List[PriceIndex]
    summary: dict


class ValidationReport(BaseModel):
    errors: List[str]

    def to_json_unescaped(self) -> str:
        """Return JSON with minimal escaping for display"""
        return json.dumps(self.model_dump(), ensure_ascii=False, indent=2)
