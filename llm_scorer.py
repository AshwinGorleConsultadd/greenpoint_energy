import os
import json
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass

# OpenAI SDK (requires openai>=1.0.0 in requirements)
from openai import OpenAI


BatchSize = 5


@dataclass
class ScoreResult:
    sequence: int
    lead_score: int
    completeness_score: float
    relevance_score: float


def load_leads(source_path: str) -> List[Dict[str, Any]]:
    with open(source_path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_output_file(path: str) -> List[Dict[str, Any]]:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump([], f)
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return []


def save_results(path: str, records: List[Dict[str, Any]]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def build_scoring_prompt(batch: List[Dict[str, Any]]) -> str:
    """Constructs a strict instruction for scoring a batch of up to 5 leads."""
    guidance = (
        "You are scoring sales leads for 'smart water valves' that reduce water consumption. "
        "Score each company on three fields strictly based on the provided data. "
        "Return ONLY a JSON list of objects, each with: sequence, lead_score, completeness_score, relevance_score.\n\n"
        "Rules:\n"
        "- lead_score is an integer 0-100 computed as:\n"
        "  Industry match (Construction/Infrastructure/Hospital/Water etc): up to +40.\n"
        "  Water/infrastructure keywords in description (water, sewer, pipeline, wastewater, etc.): up to +30.\n"
        "  Revenue quality: up to +15 (higher revenue => higher).\n"
        "  Region relevance (e.g., North America): up to +10.\n"
        "  Sufficient contact details: up to +5.\n"
        "- completeness_score is a float 0.0-1.0 reflecting richness of fields and presence of relevant contacts.\n"
        "- relevance_score is a float 0.0-1.0 for how well the company matches a buyer for smart water valves.\n"
        "- Use only the provided data and your general knowledge.\n"
        "- Never include extra fields, comments, or text.\n"
    )

    minimized = []
    for c in batch:
        minimized.append({
            "sequence": c.get("sequence"),
            "firm": c.get("firm"),
            "industry": c.get("industry"),
            "country": c.get("country"),
            "location": c.get("location"),
            "operating_regions": c.get("operating_regions", []),
            "description": c.get("description", ""),
            "total_revenue_m": c.get("total_revenue_m"),
            "contact_phones": c.get("contact_phones", []),
            "contact_emails": c.get("contact_emails", []),
            "contact_linkedins": c.get("contact_linkedins", []),
        })

    payload = json.dumps(minimized, ensure_ascii=False)
    prompt = f"{guidance}\nLeads:\n{payload}\n\nReturn JSON list only."
    return prompt


def call_gpt_for_scores(client: OpenAI, batch: List[Dict[str, Any]], model: str = "gpt-4o-mini") -> List[ScoreResult]:
    prompt = build_scoring_prompt(batch)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a precise JSON-only scoring assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
    )

    text = response.choices[0].message.content
    # Expecting a JSON object or list; when using response_format json_object, content is an object. Normalize to list.
    try:
        parsed = json.loads(text)
        # Normalize a variety of possible wrappers
        candidate_lists = []
        if isinstance(parsed, list):
            candidate_lists = [parsed]
        elif isinstance(parsed, dict):
            for key in ["results", "scores", "items", "data", "output"]:
                val = parsed.get(key)
                if isinstance(val, list):
                    candidate_lists.append(val)
            # Some models may return an object-of-objects keyed by sequence
            if not candidate_lists:
                vals = list(parsed.values())
                if vals and all(isinstance(v, dict) for v in vals):
                    candidate_lists = [vals]
        items = candidate_lists[0] if candidate_lists else []
    except Exception:
        # Fallback: extract first JSON array
        start, end = text.find("["), text.rfind("]") + 1
        items = json.loads(text[start:end]) if start >= 0 and end > start else []

    results: List[ScoreResult] = []
    for obj in items:
        try:
            results.append(
                ScoreResult(
                    sequence=int(obj.get("sequence")),
                    lead_score=int(obj.get("lead_score", 0)),
                    completeness_score=float(obj.get("completeness_score", 0.0)),
                    relevance_score=float(obj.get("relevance_score", 0.0)),
                )
            )
        except Exception:
            continue
    return results


def merge_scores(records: List[Dict[str, Any]], batch_results: List[ScoreResult]) -> None:
    seq_to_score: Dict[int, ScoreResult] = {s.sequence: s for s in batch_results}
    for rec in records:
        seq = rec.get("sequence")
        if isinstance(seq, int) and seq in seq_to_score:
            sc = seq_to_score[seq]
            rec["lead_score"] = sc.lead_score
            rec["completeness_score"] = round(sc.completeness_score, 3)
            rec["relevance_score"] = round(sc.relevance_score, 3)
        else:
            # Ensure fields exist even if not scored (fallback defaults)
            rec.setdefault("lead_score", 0)
            rec.setdefault("completeness_score", 0.0)
            rec.setdefault("relevance_score", 0.0)


def score_in_batches(
    client: OpenAI,
    companies: List[Dict[str, Any]],
    start: int,
    count: int,
    output_file: str,
    model: str = "gpt-4o-mini",
):
    total = len(companies)
    start_idx = max(0, start - 1)
    end_idx = min(start_idx + max(0, count), total) if count > 0 else total

    print(f"📁 Total companies in file: {total}")
    print(f"🚀 Scoring records from index {start} to {end_idx} (inclusive)\n")

    existing_results = ensure_output_file(output_file)
    processed_sequences = {r.get("sequence") for r in existing_results}

    # Process range using fixed-size batches
    idx = start_idx
    while idx < end_idx:
        batch_slice = companies[idx:min(idx + BatchSize, end_idx)]
        # Filter to avoid double-processing already scored sequences in output
        to_process = [c for c in batch_slice if c.get("sequence") not in processed_sequences]
        if not to_process:
            idx += BatchSize
            continue

        seqs = [c.get("sequence") for c in to_process]
        view = ", ".join(str(s) for s in seqs)
        print(f"🔍 Scoring batch: sequences [{view}] ...")

        try:
            scores = call_gpt_for_scores(client, to_process, model=model)
            # Merge back into original objects (adds defaults when missing)
            merge_scores(to_process, scores)

            # Append scored objects to existing_results and save immediately
            for c in to_process:
                existing_results.append(c)
                processed_sequences.add(c.get("sequence"))
            save_results(output_file, existing_results)

            print(f"✅ Successfully scored and saved batch: [{view}]\n")
        except Exception as e:
            # Even on error, append with default zero scores so fields are present
            for c in to_process:
                c.setdefault("lead_score", 0)
                c.setdefault("completeness_score", 0.0)
                c.setdefault("relevance_score", 0.0)
                existing_results.append(c)
                processed_sequences.add(c.get("sequence"))
            save_results(output_file, existing_results)
            print(f"❌ Error scoring batch [{view}]: {e}. Saved with default scores.\n")

        idx += BatchSize

    print(f"\n📘 Scoring complete! Saved {len(existing_results)} records to {output_file}.\n")


def main():
    source_file = os.path.join("output", "final_result.json")
    output_file = os.path.join("output", "final_scored_result.json")

    # Configure range similar to gemini_enricher.py
    start = 201    # 1-based start index
    count = 40   # how many to process; set 0 to process all from start

    # Init OpenAI client (reads OPENAI_API_KEY from env)
    client = OpenAI()

    companies = load_leads(source_file)
    score_in_batches(client, companies, start=start, count=count, output_file=output_file)


if __name__ == "__main__":
    main()


