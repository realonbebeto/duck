import re
from typing import Optional

from fastapi import HTTPException

from src.db import read_query
from src.schemas import (
    DataQualityReport,
    PriceIndex,
    PricingReport,
    PromotionReport,
    PromoUplift,
    StoreQualityIssue,
    SupplierQualityIssue,
    ValidationReport,
)


def validation_report() -> ValidationReport:
    query = "SELECT message FROM validation_errors"

    df_errors = read_query(query)

    errors = [re.sub(r'"([^"]*)"', r"[\1]", u[0]) for u in df_errors]

    return ValidationReport(errors=errors)


def quality_report() -> DataQualityReport:
    """
    Generate data quality report identifying unreliable stores and suppliers.

    Checks for:
    - Stores with high missing data rates
    - Stores with unusual sales patterns
    - Suppliers with data inconsistencies
    - Suppliers with coverage issues
    """

    store_query = """
        SELECT
            store_name,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN quantity IS NULL OR total_sales IS NULL THEN 1 ELSE 0 END) as null_sales,
            SUM(CASE WHEN quantity = 0 OR total_sales = 0 THEN 1 ELSE 0 END) as zero_sales,
            SUM(CASE WHEN quantity < 0 OR total_sales < 0 THEN 1 ELSE 0 END) as negative_values,
            AVG(total_sales/NULLIF(quantity, 0)) as avg_unit_price,
            STDDEV(total_sales/NULLIF(quantity, 0)) as price_volatility
        FROM duck_store_staging
        GROUP BY store_name
        HAVING zero_sales > total_transactions * 0.1
            OR negative_values > 0
            OR price_volatility > avg_unit_price * 2
            """

    supplier_query = """
        SELECT
            supplier,
            COUNT(*) as total_transactions,
            COUNT(DISTINCT store_name) as store_coverage,
            SUM(CASE WHEN quantity IS NULL OR total_sales IS NULL THEN 1 ELSE 0 END) as null_sales, 
            SUM(CASE WHEN quantity = 0 OR total_sales = 0 OR quantity IS NULL OR total_sales IS NULL THEN 1 ELSE 0 END) as zero_sales,
            SUM(CASE WHEN rrp IS NULL OR rrp = 0 THEN 1 ELSE 0 END) as missing_rrp
        FROM duck_store_staging
        GROUP BY supplier
        HAVING zero_sales > total_transactions * 0.15
            OR missing_rrp > total_transactions * 0.15
            OR store_coverage < 3
            """
    total_stores = read_query(
        "SELECT COUNT(DISTINCT store_name) AS count FROM duck_store_staging"
    )[0][0]
    total_suppliers = read_query(
        "SELECT COUNT(DISTINCT supplier) AS count FROM duck_store_staging"
    )[0][0]

    unreliable_stores = []
    unreliable_suppliers = []

    df_store = read_query(store_query)
    df_supplier = read_query(supplier_query)

    overall_store_health = None

    if df_store:
        for row in df_store:
            store_name, total, nulls, zeros, negatives, avg_price, volatility = row

            issues = []

            if negatives > 0:
                issues.append(f"{negatives} negative values")

            if nulls > total * 0.2:
                issues.append(f"{nulls} nulls")

            if zeros > total * 0.2:
                issues.append(f"{zeros} zero sales ({zeros * 100 / total:.1f}%)")

            if volatility and avg_price and volatility > avg_price * 2:
                issues.append("Volatility is greater than 2X of average price")

            unreliable_stores.append(
                StoreQualityIssue(
                    store_name=store_name,
                    issue_type="data_quality",
                    details={
                        "total_transactions": total,
                        "zero_sales": zeros,
                        "negative_values": negatives,
                        "issues": issues,
                    },
                )
            )

        overall_store_health = (
            "good" if len(unreliable_stores) < total_stores * 0.1 else "needs_attention"
        )

    overal_supplier_health = None
    if df_supplier:
        for row in df_supplier:
            supplier, total, coverage, nulls, zeros, missing_rrp = row

            issues = []

            if missing_rrp > total * 0.2:
                issues.append(f"{missing_rrp} missing rrp")

            if nulls > total * 0.2:
                issues.append(f"{nulls} nulls")

            if zeros > total * 0.2:
                issues.append(f"{zeros} zero sales")

            if coverage < 3:
                issues.append(f"Low store coverage: {coverage} stores")

            unreliable_suppliers.append(
                SupplierQualityIssue(
                    supplier=supplier,
                    issue_type="data quality",
                    details={
                        "total_transactions": total,
                        "store_coverage": coverage,
                        "issues": issues,
                    },
                )
            )
        overal_supplier_health = (
            "good"
            if len(unreliable_suppliers) < total_suppliers * 0.1
            else "needs_attention"
        )

    return DataQualityReport(
        unreliable_stores=unreliable_stores,
        unreliable_suppliers=unreliable_suppliers,
        summary={
            "total_stores_analyzed": total_stores,
            "total_suppliers_analyzed": total_suppliers,
            "unreliable_stores_count": len(unreliable_stores),
            "unreliable_stores_pct": round(
                (len(unreliable_stores) / total_stores) * 100, 2
            ),
            "unreliable_suppliers_count": len(unreliable_suppliers),
            "unreliable_suppliers_pct": round(
                (len(unreliable_suppliers) / total_suppliers) * 100, 2
            ),
            "overall_store_health": overall_store_health,
            "overal_supplier_health": overal_supplier_health,
        },
    )


def promotion_report(supplier: str, min_uplift: Optional[float] = None):
    """
    Generate promotion uplift analysis comparing promotional units vs baseline.

    Parameters:
    - supplier: str filtet for supplier in question
    - min_uplift: Optional filter for minimum uplift percentage
    """
    query = f"""
            SELECT
                description,
                section,
                -- Promo sales (discounted)
                SUM(CASE WHEN sale_price <= rrp * 0.90 THEN quantity ELSE 0 END) as promo_units,
                -- Regular sales (baseline)
                SUM(CASE WHEN sale_price >= rrp * 0.90 THEN quantity ELSE 0 END) as baseline_units,
                -- Uplift calculation
                ROUND(((SUM(CASE WHEN sale_price <= rrp * 0.90 THEN quantity ELSE 0 END) -
                SUM(CASE WHEN sale_price >= rrp * 0.90 THEN quantity ELSE 0 END)) /
                NULLIF(SUM(CASE WHEN sale_price >= rrp * 0.90 THEN quantity ELSE 0 END), 0)) * 100, 2) as uplift_pct,
                COUNT(CASE WHEN sale_price <= rrp * 0.90 THEN 1 END) as promo_transactions
            FROM duck_store
            WHERE rrp IS NOT NULL AND sale_price IS NOT NULL
            AND CONTAINS(LOWER(supplier), '{supplier}')
            GROUP BY description, section
            HAVING promo_units > 0 AND baseline_units > 0
            ORDER BY uplift_pct DESC
        """

    df_promo = read_query(query)

    if not df_promo:
        raise HTTPException(
            status_code=204, detail=f"No data found for brand '{supplier}'"
        )

    uplifts = []
    for row in df_promo:
        desc, section, promo_units, baseline_units, uplift_pct, promo_txs = row

        if min_uplift is None or uplift_pct >= min_uplift:
            uplifts.append(
                PromoUplift(
                    product_name=desc,
                    promo_units=promo_units,
                    section=section,
                    uplift_pct=uplift_pct,
                    total_promo_transactions=promo_txs,
                    baseline_units=baseline_units,
                )
            )

    uplifts.sort(key=lambda x: x.uplift_pct, reverse=True)

    total_promo_units = sum(u.promo_units for u in uplifts)
    total_baseline = sum(u.baseline_units for u in uplifts)
    avg_uplift = sum(u.uplift_pct for u in uplifts) / len(uplifts) if uplifts else 0

    return PromotionReport(
        summary={
            "total_promoted_products": len(uplifts),
            "total_promo_units": total_promo_units,
            "total_baseline_units": total_baseline,
            "overall_uplift_unit_pct": round(
                (((total_promo_units - total_baseline) / total_baseline) * 100), 2
            )
            if total_baseline > 0
            else 0,
            "average_uplift_pct": round(avg_uplift, 2),
            "positive_uplift_count": len([u for u in uplifts if u.uplift_pct > 0]),
            "negative_uplift_count": len([u for u in uplifts if u.uplift_pct < 0]),
        },
        top_performers=uplifts[:10],
        poor_performers=uplifts[-10:][::-1],
    )


def pricing_report(supplier: str) -> PricingReport:
    """
    Generate pricing report comparing target supplier's average unit price vs competitors
    within the same sub-department and section, per store.

    Parameters:
    - supplier: Brand name to analyze
    """
    query = """
        WITH item_prices AS (
            SELECT
                store_name,
                sub_department,
                section,
                supplier,
                AVG(total_sales / NULLIF(quantity, 0)) as avg_unit_price,
                SUM(quantity) as total_units
            FROM duck_store
                WHERE quantity > 0 AND total_sales > 0
                GROUP BY store_name, sub_department, section, supplier
            ),
            competitor_prices AS (
                SELECT
                    store_name,
                    sub_department,
                    section,
                    AVG(avg_unit_price) as competitor_avg_price
                FROM item_prices
                    WHERE NOT CONTAINS(LOWER(supplier), 'bidco')
                GROUP BY store_name, sub_department, section
            )
        SELECT
            ip.supplier,
            ip.store_name,
            ip.sub_department,
            ip.section,
            ROUND(ip.avg_unit_price, 2) as supplier_avg_price,
            ROUND(cp.competitor_avg_price, 2) as competitor_avg_price,
            ROUND((ip.avg_unit_price/NULLIF(cp.competitor_avg_price, 0)) * 100, 2) as price_index
        FROM item_prices ip
            LEFT JOIN competitor_prices cp
            ON ip.store_name = cp.store_name
            AND ip.section = cp.section
            AND ip.sub_department = cp.sub_department
        WHERE CONTAINS(LOWER(ip.supplier), 'bidco')
        AND cp.competitor_avg_price IS NOT NULL
        ORDER BY price_index DESC
            """

    df_price = read_query(query)

    if not df_price:
        raise HTTPException(
            status_code=204, detail=f"No data found for brand '{supplier}'"
        )

    price_indices = []

    for row in df_price:
        (
            supplier,
            store_name,
            sub_department,
            section,
            supplier_avg_price,
            competitor_avg_price,
            price_index,
        ) = row

        if price_index < 90:
            position = "discount"
        elif price_index > 105:
            position = "premium"
        else:
            position = "market"

        price_indices.append(
            PriceIndex(
                store_name=store_name,
                sub_department=sub_department,
                section=section,
                supplier_avg_price=supplier_avg_price,
                competitor_avg_price=competitor_avg_price,
                price_index=price_index,
                market_position=position,
            )
        )

    avg_index = sum(p.price_index for p in price_indices) / len(price_indices)

    return PricingReport(
        supplier_pricing=price_indices,
        summary={
            "supplier": supplier,
            "total_store_segments": len(price_indices),
            "average_price_index": round(avg_index, 2),
            "premium_positions": len(
                [p for p in price_indices if p.market_position == "premium"]
            ),
            "market_positions": len(
                [p for p in price_indices if p.market_position == "market"]
            ),
            "discount_positions": len(
                [p for p in price_indices if p.market_position == "discount"]
            ),
            "price_competitiveness": "competitive"
            if 90 <= avg_index <= 105
            else ("premium" if avg_index > 105 else "discount"),
        },
    )
