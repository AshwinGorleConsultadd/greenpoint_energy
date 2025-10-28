"""
llm_enricher.py
Uses OpenAI to fill missing fields and infer contacts, with light web context.
"""

from typing import Dict, Any, Optional
import json
import os
from config import OPENAI_API_KEY
import logging


TARGET_FIELDS = [
    "industry",
    "founded_year",
    "headquarters",
    "country",
    "website",
    "employee_count",
    "annual_turnover",
    "description",
    "operating_regions",
    "contact_name",
    "contact_designation",
    "contact_email",
    "contact_phone",
    "linkedin_url",
    "related_contacts",
]


def _safe_client() -> Optional[Any]:
    api_key = OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        import importlib
        openai_mod = importlib.import_module("openai")
        OpenAI = getattr(openai_mod, "OpenAI")
        return OpenAI(api_key=api_key)
    except Exception:
        return None


SYSTEM_PROMPT = (
    "You are a precise B2B data enricher for construction and infrastructure firms. "
    "Infer and complete missing fields ONLY from trusted public context. "
    "Return STRICT JSON with the specified schema. Use null when unknown."
)


def enrich_with_llm(base_record: Dict[str, Any], ddg_enrichment: Dict[str, Any]) -> Dict[str, Any]:
    """Use LLM to fill missing fields using provided record and any DDG context."""
    logger = logging.getLogger("llm")
    client = _safe_client()
    if client is None:
        # Fallback: return base merged with ddg_enrichment; unknowns stay None
        logger.info("OpenAI client unavailable; using DDG-only enrichment fallback")
        merged = {f: ddg_enrichment.get(f) for f in TARGET_FIELDS}
        return {k: merged.get(k) for k in TARGET_FIELDS}

    user_payload = {
        "base": base_record,
        "duckduckgo": ddg_enrichment,
        "required_fields": TARGET_FIELDS,
        "instructions": "Fill missing fields from reliable public data. Provide contact arrays as specified.",
    }

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.2,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": "Return only JSON matching this schema (no commentary).\n" + json.dumps(user_payload),
                },
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content)
        # Keep only target fields; ensure types
        result: Dict[str, Any] = {k: parsed.get(k) for k in TARGET_FIELDS}
        logger.debug("LLM enrichment received and parsed successfully")
        return result
    except Exception:
        # Fail-safe: return DDG only mapping
        logger.warning("LLM enrichment failed; falling back to DDG-only fields")
        return {k: ddg_enrichment.get(k) for k in TARGET_FIELDS}


def enrich_batch_with_llm(records: list[Dict[str, Any]], batch_size: int = 10) -> list[Dict[str, Any]]:
    """Enrich all records using a batched LLM approach and compute scores; updates in place.

    - records: full list of records
    - batch_size: max records per LLM call
    Returns the same list updated with required fields and scores.
    """
    logger = logging.getLogger("llm")
    total = len(records)
    if total == 0:
        return records
    client = _safe_client()
    if client is None:
        logger.info("OpenAI client unavailable for batch; attaching scores only for %d records", total)
        for rec in records:
            scores = score_record({k: rec.get(k) for k in TARGET_FIELDS})
            rec.update(scores)
        return records

    logger.info("[LLM] Starting enrichment for %d records (chunk size=%d)", total, batch_size)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        chunk = records[start:end]
        payload = {
            "instructions": "Fill missing fields for each firm. Return a JSON array with one object per input record, in the same order. Use null when unknown.",
            "required_fields": TARGET_FIELDS,
            "records": chunk,
        }
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.2,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload)},
                ],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content or "{}"
            parsed = json.loads(content)
            enriched_list = parsed.get("records")
            if not isinstance(enriched_list, list) or len(enriched_list) != len(chunk):
                logger.warning("LLM chunk response mismatch for %d-%d; skipping update", start + 1, end)
                enriched_list = [{k: chunk[i].get(k) for k in TARGET_FIELDS} for i in range(len(chunk))]
            for i, llm_obj in enumerate(enriched_list):
                rec = chunk[i]
                for k in TARGET_FIELDS:
                    rec[k] = llm_obj.get(k, rec.get(k))
                scores = score_record({k: rec.get(k) for k in TARGET_FIELDS})
                rec.update(scores)
            logger.debug("[LLM] Completed chunk %d-%d/%d", start + 1, end, total)
        except Exception:
            logger.warning("LLM chunk %d-%d failed; attaching scores only", start + 1, end)
            for rec in chunk:
                scores = score_record({k: rec.get(k) for k in TARGET_FIELDS})
                rec.update(scores)
    logger.info("[LLM] Enrichment completed for %d records", total)
    return records


def score_record(enriched: Dict[str, Any]) -> Dict[str, Any]:
    """Compute heuristic scores; may be refined with LLM later if desired."""
    logger = logging.getLogger("scoring")
    # Data completeness
    total = len(TARGET_FIELDS)
    filled = sum(1 for k in TARGET_FIELDS if enriched.get(k) not in (None, "", []))
    data_completeness_score = round(100 * filled / max(1, total))

    # Water focus proxy: description and industry keywords
    text = (enriched.get("description") or "") + " " + (enriched.get("industry") or "")
    text_lower = str(text).lower()
    water_keywords = ["water", "wastewater", "water supply", "sewer"]
    infra_keywords = ["infrastructure", "transportation", "power", "industrial"]
    water_focus_score = min(100, 25 * sum(1 for k in water_keywords if k in text_lower))
    infra_focus_score = min(100, 25 * sum(1 for k in infra_keywords if k in text_lower))

    # Lead score combining simple signals
    lead_score = int(0.4 * water_focus_score + 0.4 * infra_focus_score + 0.2 * data_completeness_score)

    scores = {
        "water_focus_score": water_focus_score,
        "infra_focus_score": infra_focus_score,
        "lead_score": lead_score,
        "data_completeness_score": data_completeness_score,
    }
    logger.debug(f"Scores computed: {scores}")
    return scores


