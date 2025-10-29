import os
import json
from google import genai
from google.genai import types
# --- SETUP ---
print("Gemini API Key Loaded :white_tick:")
grounding_tool = types.Tool(
    google_search=types.GoogleSearch()
)
client = genai.Client()
# --- SAMPLE DATA ---
companies = [{
    "rank_2025": 1.0,
    "rank_2024": 1.0,
    "firm": "TURNER CONSTRUCTION CO., New York, N.Y.",
    "total_revenue_m": "20,241.4",
    "int_total_revenue_m": "591.9",
    "website": "https://www.turnerconstruction.com",
    "headquarters": "New York, N.Y.",
    "country": "USA",
    "description": "Turner Construction Company is a North American-based, international construction services company.",
    "industry": "Construction",
    "founded_year": 1902
}]
def enrich_company_contacts(company):
    """
    Uses Gemini with search grounding to find public contact info.
    """
    firm = company["firm"]
    website = company.get("website", "")
    prompt = f"""
    Use live web search to find publicly available contact information for executives
    or senior management at {firm} ({website}).
    Return a structured JSON object in this format:
    {{
      "contact_phones": [{{"contact_name": "", "phone": "", designarion : ""}}],
      "contact_emails": [{{"contact_name": "", "email": "", designarion : ""}}],
      "contact_linkedins": [{{"contact_name": "", "linkedin_url": "", designarion : ""}}]
    }}
    Only include public business contacts. If not found, return empty arrays.
    """
    try:
        config = types.GenerateContentConfig(
    tools=[grounding_tool]
)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=config,
        )
        # response = model.generate_content(prompt)
        print("model response is ", response)
        text = response.text.strip()
        json_text = text[text.find("{"): text.rfind("}") + 1]
        contact_info = json.loads(json_text)
        company.update(contact_info)
        return company
    except Exception as e:
        company["error"] = str(e)
        return company
def main():
    enriched_data = []
    for c in companies:
        print(f":magnifying_glass: Enriching {c['firm']} ...")
        enriched_data.append(enrich_company_contacts(c))
    # print(json.dumps(enriched_data, indent=2))
if __name__ == "__main__":
    main()