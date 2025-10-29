from logging import StringTemplateStyle
import os
import json
from google import genai
from google.genai import types
# --- SETUP ---
os.environ["GEMINI_API_KEY"] = ""
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
grounding_tool = types.Tool(google_search=types.GoogleSearch())
def enrich_company_contacts(company):
    """
    Uses Gemini 2.5 with live web grounding to enrich company info.
    Input: only the firm name.
    Output: structured data including website, HQ, country, etc.
    """
    firm = company["firm"]
    prompt = f"""
    Use live web search to find verified, up-to-date business information about the company "{firm}".
    Return a structured JSON object in this format:
    {{
    "firm: ""
    "website": "",
    "headquarters": "",
    "country": "",
    "description": "",
    "industry": "",
    "founded_year": "",
    "employee_count": "",
    "revenue_m": "",
    "operating_regions": [],  # max 3 regions only
    "specializations": [],   # max 3 specializations only
    "notable_projects": [], # max 4 notable_projects only
    "key_people": [{{"name": "", "position": ""}}]
    "contact_phones": [{{"contact_name": "", "phone": "", "designation": ""}}],
    "contact_emails": [{{"contact_name": "", "email": "", "designation": ""}}],
    "contact_linkedins": [{{"contact_name": "", "linkedin_url": "", "designation": ""}}]
    }}
    Only include verified public data.
    Omit any fields that are not publicly available.
    Do NOT repeat input values like the company name.
    """
    try:
        # --- Primary call with grounding ---
        config = types.GenerateContentConfig(tools=[grounding_tool])
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=config,
        )
        text = response.text.strip()
        start, end = text.find("{"), text.rfind("}") + 1
        json_text = text[start:end]
        contact_info = json.loads(json_text)
        company.update(contact_info)
        return company
    except Exception as e:
        print(f":warning: Primary enrichment failed for {firm}, retrying with schema-enforced fallback. Error: {e}")
        try:
            # --- Fallback schema-enforced JSON call ---
            fallback_prompt = f"""
            Extract structured public contact details for executives or senior management of {firm}.
            Return strictly JSON following the required schema.
            """
            contents = [
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=fallback_prompt)],
                ),
            ]
            generate_content_config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=genai.types.Schema(
                    type=genai.types.Type.OBJECT,
                    required=["contact_phones", "contact_emails", "contact_linkedins"],
                    properties={
                        "firm":  genai.types.Schema(type=genai.types.Type.STRING),
                        "website": genai.types.Schema(type=genai.types.Type.STRING),
                        "headquarters": genai.types.Schema(type=genai.types.Type.STRING),
                        "country": genai.types.Schema(type=genai.types.Type.STRING),
                        "description": genai.types.Schema(type=genai.types.Type.STRING),
                        "industry": genai.types.Schema(type=genai.types.Type.STRING),
                        "founded_year": genai.types.Schema(type=genai.types.Type.STRING),
                        "employee_count": genai.types.Schema(type=genai.types.Type.STRING),
                        "revenue_m": genai.types.Schema(type=genai.types.Type.STRING),
                        
                        # fix code for these fields these are array of StringTemplateStyle
                        "operating_regions": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        "specializations": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),
                        "notable_projects": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(type=genai.types.Type.STRING)
                        ),

                        "key_people": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(
                                type=genai.types.Type.OBJECT,
                                required=["name", "position"],
                                properties={
                                    "name": genai.types.Schema(type=genai.types.Type.STRING),
                                    "position": genai.types.Schema(type=genai.types.Type.STRING),
                                },
                            ),
                        ),
                       
                        "contact_phones": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(
                                type=genai.types.Type.OBJECT,
                                required=["contact_name", "phone", "designation"],
                                properties={
                                    "contact_name": genai.types.Schema(type=genai.types.Type.STRING),
                                    "phone": genai.types.Schema(type=genai.types.Type.STRING),
                                    "designation": genai.types.Schema(type=genai.types.Type.STRING),
                                },
                            ),
                        ),
                        "contact_emails": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(
                                type=genai.types.Type.OBJECT,
                                required=["contact_name", "email", "designation"],
                                properties={
                                    "contact_name": genai.types.Schema(type=genai.types.Type.STRING),
                                    "email": genai.types.Schema(type=genai.types.Type.STRING),
                                    "designation": genai.types.Schema(type=genai.types.Type.STRING),
                                },
                            ),
                        ),
                        "contact_linkedins": genai.types.Schema(
                            type=genai.types.Type.ARRAY,
                            items=genai.types.Schema(
                                type=genai.types.Type.OBJECT,
                                required=["contact_name", "linkedin_url", "designation"],
                                properties={
                                    "contact_name": genai.types.Schema(type=genai.types.Type.STRING),
                                    "linkedin_url": genai.types.Schema(type=genai.types.Type.STRING),
                                    "designation": genai.types.Schema(type=genai.types.Type.STRING),
                                },
                            ),
                        ),
                    },
                ),
            )
            response = client.models.generate_content(
                model="gemini-2.5-pro",
                contents=contents,
                config=generate_content_config,
            )
            fallback_data = json.loads(response.text)
            company["contacts"] = fallback_data
            return company
        except Exception as e2:
            company["error"] = f"Both enrichment methods failed: {e2}"
            return company
# def main():
#     # :white_tick: Input: only firm name
#     # companies = [
#     #     {
#     #         "firm": "TURNER CONSTRUCTION CO., New York, N.Y.",
#     #         "enr_rank_2025": 1.0,
#     #         "enr_rank_2024": 1.0,
#     #         "total_revenue_m": "20,241.4",
#     #         "int_total_revenue_m": "591.9",
#     #         "new_contracts": "26,136.4",
#     #         "general_building_pct": 60.0,
#     #         "water_supply_pct": 0.0,
#     #         "location": "New York, N.Y."
#     #     },
#     #     # {"firm": "Turner Construction Co."},
#     #     # {"firm": "Bechtel Corporation"}
#     # ]

#     preserve_fields = [
#         "enr_rank_2025",
#         "enr_rank_2024",
#         "total_revenue_m",
#         "int_total_revenue_m",
#         "new_contracts",
#         "general_building_pct",
#         "water_supply_pct",
#         "location"
#     ]

#     file_path = "output/step2_celaned_data.json"
#     with open(file_path, "r", encoding="utf-8") as f:
#         companies = json.load(f)
#     enriched_data = []
#     total = len(companies)
#     for idx, c in enumerate(companies, start=1):
#         if(idx < 10) :
#             print(f"🔍 [{idx}/{total}] Enriching {c['firm']} ...")
#             enriched_record = enrich_company_contacts(c)

#             # Preserve original fields
#             for field in preserve_fields:
#                 if field in c:
#                     enriched_record[field] = c[field]

#             enriched_data.append(enriched_record)
#     # Pretty-print final result
#     print("\n:blue_book: Final Enriched Output:")
#     print(json.dumps(enriched_data, indent=2))
#     return enriched_record

def gemini_enricher(records):
    print("demo")

def main():
    file_path = "output/step2_celaned_data.json"
    output_file = "output/final_result.json"

    # Load source companies
    with open(file_path, "r", encoding="utf-8") as f:
        companies = json.load(f)

    total = len(companies)
    print(f"📁 Total companies in file: {total}")

    # ✅ 1. Configure range
    start = 91   # <-- starting index (1-based)
    count = 40      # <-- number of records to process from start
    end = min(start - 1 + count, total)
    print(f"🚀 Processing records from index {start} to {end} (inclusive)\n")

    preserve_fields = [
        "enr_rank_2025",
        "enr_rank_2024",
        "total_revenue_m",
        "int_total_revenue_m",
        "new_contracts",
        "general_building_pct",
        "water_supply_pct",
        "location"
    ]

    # Ensure output directory and file exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if not os.path.exists(output_file):
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([], f)

    # Load any previously enriched data (so we can append)
    with open(output_file, "r", encoding="utf-8") as f:
        existing_results = json.load(f)

    for idx in range(start - 1, end):
        c = companies[idx]
        print(f"🔍 [{idx + 1}/{total}] Enriching {c['firm']} ...")

        try:
            enriched_record = enrich_company_contacts(c)

            # ✅ 3. Add sequence number
            enriched_record["sequence"] = idx + 1

            # Preserve original fields
            for field in preserve_fields:
                if field in c:
                    enriched_record[field] = c[field]

            # ✅ 4. Save immediately after each success
            existing_results.append(enriched_record)
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(existing_results, f, ensure_ascii=False, indent=2)

            print(f"✅ Successfully enriched and saved: {c['firm']} (Seq #{idx + 1})\n")

        except Exception as e:
            print(f"❌ Error enriching {c['firm']}: {e}\n")

    print(f"\n📘 Processing complete! Saved {len(existing_results)} records to {output_file}.\n")
    return existing_results


if __name__ == "__main__":
    main()

