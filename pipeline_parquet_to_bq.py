# pipeline_parquet_to_bq.py
# Advanced Beam batch pipeline with configurable knobs:
# - Read Parquet from GCS (supports globs)
# - Parse & normalize (case/whitespace/regex/type coercion)
# - Validate with side-outputs (dead-letter invalid rows to GCS JSONL)
# - Deduplicate by transaction_id (keep latest transaction_ts)
# - Enrich with product lookup (side input JSON/JSONL)
# - FX conversion & amount
# - Write to BigQuery with optional partitioning & clustering


import argparse
import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

FX_RATES = {"USD": 1.0, "EUR": 1.07, "GBP": 1.26}
ISO_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"

def _to_float_safe(x) -> float:
    if x is None:
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0

def _to_ts_safe(x: Any) -> str:
    """Return RFC3339 string if we can parse, else empty string."""
    if isinstance(x, str) and re.search(r"Z$|[+-]\d\d:\d\d$", x):
        return x
    try:
        s = str(x).replace(" ", "T")
        if not s.endswith("Z") and re.match(r"\d{4}-\d{2}-\d{2}T", s):
            s += "Z"
        datetime.strptime(s, ISO_TS_FMT)
        return s
    except Exception:
        return ""

class ParseAndNormalize(beam.DoFn):
    def __init__(self, currency: str = "USD"):
        self.currency = currency
        self.parsed_counter = Metrics.counter(self.__class__, "rows_parsed")

    def process(self, row: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        row = {str(k).strip().lower(): v for k, v in row.items()}
        txn_id = re.sub(r"[^a-zA-Z0-9\-]", "", str(row.get("transaction_id") or row.get("id") or "").strip())
        cust_id = str(row.get("customer_id") or "").strip()
        prod_id = re.sub(r"\s+", "-", str(row.get("product_id") or "").strip())
        quantity = _to_float_safe(row.get("quantity"))
        unit_price = _to_float_safe(row.get("unit_price"))
        transaction_ts = _to_ts_safe(row.get("transaction_ts"))

        out = {
            "transaction_id": txn_id,
            "customer_id": cust_id,
            "product_id": prod_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "transaction_ts": transaction_ts,
            "currency": self.currency,
        }
        self.parsed_counter.inc()
        yield out

class Validate(beam.DoFn):
    INVALID_TAG = "invalid"

    def __init__(self, required_fields, require_non_negative=True, max_unit_price=None):
        self.required_fields = required_fields or ["transaction_id", "product_id", "quantity", "unit_price"]
        self.require_non_negative = bool(require_non_negative)
        self.max_unit_price = float(max_unit_price) if max_unit_price is not None else None
        self.valid_counter = Metrics.counter(self.__class__, "rows_valid")
        self.invalid_counter = Metrics.counter(self.__class__, "rows_invalid")

    def process(self, row: Dict[str, Any]):
        errs = []
        for f in self.required_fields:
            if row.get(f) in (None, "", []):
                errs.append(f"missing_{f}")
        if self.require_non_negative:
            if row.get("quantity", 0) < 0:
                errs.append("neg_quantity")
            if row.get("unit_price", 0) < 0:
                errs.append("neg_unit_price")
        if self.max_unit_price is not None and row.get("unit_price", 0) > self.max_unit_price:
            errs.append("unit_price_gt_max")

        if errs:
            self.invalid_counter.inc()
            bad = dict(row)
            bad["validation_errors"] = errs
            yield pvalue.TaggedOutput(self.INVALID_TAG, bad)
        else:
            self.valid_counter.inc()
            yield row

class DeduplicateByTransaction(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "KeyByTxn" >> beam.Map(lambda r: (r["transaction_id"], r))
            | "KeepLatest" >> beam.CombinePerKey(lambda rows: max(rows, key=lambda r: r.get("transaction_ts") or ""))
            | "DropKey" >> beam.Map(lambda kv: kv[1])
        )

class ComputeAmount(beam.DoFn):
    def __init__(self):
        self.fx_miss_counter = Metrics.counter(self.__class__, "fx_misses")

    def process(self, row: Dict[str, Any]):
        currency = row.get("currency", "USD")
        fx = FX_RATES.get(currency)
        if fx is None:
            self.fx_miss_counter.inc()
            fx = 1.0
        amt = _to_float_safe(row.get("quantity")) * _to_float_safe(row.get("unit_price")) * fx
        row["amount"] = round(amt, 2)
        yield row

class EnrichWithLookup(beam.DoFn):
    def process(self, row: Dict[str, Any], product_map: Dict[str, Dict[str, Any]]):
        info = product_map.get(row.get("product_id", ""), {})
        promos = info.get("promo_skus", [])
        row["product_category"] = info.get("category", "unknown")
        row["is_promo"] = row.get("product_id") in promos
        yield row

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS URI to Parquet (can include * globs).")
    parser.add_argument("--output_table", required=True, help="BigQuery table: project:dataset.table")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--lookup_path", help="GCS path to product lookup (JSON/JSONL).")
    parser.add_argument("--deadletter_path", help="GCS prefix for invalid rows JSONL (e.g. gs://bucket/bad/20250101/bad-*.jsonl)")
    parser.add_argument("--required_fields", default="transaction_id,product_id,quantity,unit_price")
    parser.add_argument("--require_non_negative", type=lambda s: s.lower() != "false", default=True)
    parser.add_argument("--max_unit_price", type=float, default=None)
    parser.add_argument("--output_write_disposition", default="WRITE_APPEND",
                        choices=["WRITE_APPEND", "WRITE_TRUNCATE"])
    parser.add_argument("--output_create_disposition", default="CREATE_IF_NEEDED",
                        choices=["CREATE_IF_NEEDED", "CREATE_NEVER"])
    parser.add_argument("--bq_partition_field", default=None, help="Field name to partition on (e.g., transaction_ts)")
    parser.add_argument("--bq_cluster_fields", default=None, help="Comma-separated cluster fields (e.g., product_id,customer_id)")

    known_args, pipeline_args = parser.parse_known_args(argv)

    required_fields = [f.strip() for f in known_args.required_fields.split(",") if f.strip()]

    schema = {
        "fields": [
            {"name": "transaction_id",  "type": "STRING"},
            {"name": "customer_id",     "type": "STRING"},
            {"name": "product_id",      "type": "STRING"},
            {"name": "quantity",        "type": "FLOAT"},
            {"name": "unit_price",      "type": "FLOAT"},
            {"name": "transaction_ts",  "type": "TIMESTAMP"},
            {"name": "currency",        "type": "STRING"},
            {"name": "amount",          "type": "FLOAT"},
            {"name": "product_category","type": "STRING"},
            {"name": "is_promo",        "type": "BOOL"},
        ]
    }

    # Build BigQuery additional parameters (Beam 2.67 style)
    additional_bq_parameters: Dict[str, Any] = {}
    if known_args.bq_partition_field:
        additional_bq_parameters["timePartitioning"] = {"type": "DAY", "field": known_args.bq_partition_field}
    if known_args.bq_cluster_fields:
        additional_bq_parameters["clustering"] = {
            "fields": [c.strip() for c in known_args.bq_cluster_fields.split(",") if c.strip()]
        }

    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=False)

    with beam.Pipeline(options=options) as p:
        # Side input (lookup)
        if known_args.lookup_path:
            lookup_lines = p | "ReadLookup" >> beam.io.ReadFromText(known_args.lookup_path)
            product_dicts = lookup_lines | "ParseLookupJSON" >> beam.Map(json.loads)
            product_map_pcoll = (
                product_dicts
                | "MergeLookupDicts" >> beam.CombineGlobally(
                    lambda dicts: {k: v for d in dicts for k, v in d.items()}
                )
            )
        else:
            product_map_pcoll = p | "EmptyLookup" >> beam.Create([{}])

        # Read
        rows = p | "ReadParquet" >> beam.io.ReadFromParquet(known_args.input)

        # Transformations
        normalized = rows | "ParseNormalize" >> beam.ParDo(ParseAndNormalize(currency=known_args.currency))
        validated = normalized | "Validate" >> beam.ParDo(
            Validate(
                required_fields=required_fields,
                require_non_negative=known_args.require_non_negative,
                max_unit_price=known_args.max_unit_price,
            )
        ).with_outputs(Validate.INVALID_TAG, main="valid")

        valid_rows = validated["valid"]
        invalid_rows = validated[Validate.INVALID_TAG]

        deduped  = valid_rows | "DedupByTxn" >> DeduplicateByTransaction()
        enriched = deduped    | "Enrich"     >> beam.ParDo(EnrichWithLookup(), pvalue.AsSingleton(product_map_pcoll))
        computed = enriched   | "ComputeAmt" >> beam.ParDo(ComputeAmount())

        # Write to BigQuery (Beam 2.67: use additional_bq_parameters)
        computed | "WriteToBQ" >> WriteToBigQuery(
            known_args.output_table,
            schema=schema,
            write_disposition=getattr(beam.io.gcp.bigquery.BigQueryDisposition, known_args.output_write_disposition),
            create_disposition=getattr(beam.io.gcp.bigquery.BigQueryDisposition, known_args.output_create_disposition),
            additional_bq_parameters=additional_bq_parameters or None,
        )

        # Dead-letter invalid rows
        if known_args.deadletter_path:
            (
                invalid_rows
                | "InvalidToJSON" >> beam.Map(json.dumps)
                | "WriteBadRows" >> beam.io.WriteToText(
                    known_args.deadletter_path,
                    file_name_suffix=".jsonl",
                    shard_name_template="-SSSS-of-NNNN"
                )
            )

if __name__ == "__main__":
    run()
