import json
import os
from typing import List, Dict, Any, Tuple
import streamlit as st
import pandas as pd

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
    AGGRID_AVAILABLE = True
except Exception:
    AGGRID_AVAILABLE = False

st.set_page_config(page_title="Leads Explorer", page_icon="📘", layout="wide")


def inject_styles():
    st.markdown(
        """
        <style>
        /* Make table and headers consistent across environments */
        .ag-theme-balham, .ag-theme-balham .ag-root, .ag-theme-balham .ag-header-cell-label { font-size: 14px !important; }
        .ag-theme-balham .ag-header-cell-text { font-weight: 600 !important; }
        .stDataFrame, .stDataEditor, .stDataFrame table, .stDataEditor table, .stDataFrame th, .stDataEditor th, .stDataFrame td, .stDataEditor td { font-size: 14px !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

def load_data() -> List[Dict[str, Any]]:
    """Load leads data from output/final_scored_result.json if available, else final_result.json, else a static sample."""
    scored_path = os.path.join("output", "final_scored_result.json")
    base_path = os.path.join("output", "final_result.json")
    if os.path.exists(scored_path):
        with open(scored_path, "r", encoding="utf-8") as f:
            return json.load(f)
    if os.path.exists(base_path):
        with open(base_path, "r", encoding="utf-8") as f:
            return json.load(f)
    # Static fallback (single record)
    return [
        {
            "enr_rank_2025": 1.0,
            "enr_rank_2024": 1.0,
            "firm": "TURNER CONSTRUCTION CO.",
            "total_revenue_m": "20,241.4",
            "int_total_revenue_m": "591.9",
            "new_contracts": "26,136.4",
            "general_building_pct": 60.0,
            "water_supply_pct": 0.0,
            "location": "New York, N.Y.",
            "website": "turnerconstruction.com",
            "headquarters": "375 Hudson Street, New York, NY 10014, United States",
            "country": "United States",
            "description": "Turner Construction Company is a North America-based, international construction services company and a leading builder in diverse market segments.",
            "industry": "Construction",
            "founded_year": "1902",
            "employee_count": "18000",
            "revenue_m": "13400",
            "operating_regions": ["North America", "Africa", "Asia", "Europe", "India"],
            "specializations": ["Construction Management", "General Contracting", "Consulting", "Green Building", "BIM"],
            "notable_projects": ["United Nations Secretariat Building", "Madison Square Garden", "SoFi Stadium (Los Angeles)", "Intuit Dome (Los Angeles)"],
            "key_people": [{"name": "Peter Davoren", "position": "Chairman and CEO"}, {"name": "Christa Andresky", "position": "EVP & CFO"}],
            "contact_phones": [{"contact_name": "Headquarters", "phone": "(212) 229-6000", "designation": "General"}],
            "contact_emails": [{"contact_name": "General", "email": "turner@tcco.com", "designation": "General Inquiry"}],
            "contact_linkedins": [],
            "sequence": 1,
        }
    ]


def tagline(text: str):
    st.markdown(f"<div style='display:inline-block;padding:4px 10px;margin:2px;border-radius:12px;background:#EEF2FF;color:#3730A3;font-size:12px;border:1px solid #C7D2FE;'>{text}</div>", unsafe_allow_html=True)


def render_header(detail: bool = False):
    col1, col2 = st.columns([0.8, 0.2])
    with col1:
        st.title("📘 Leads Explorer" if not detail else "📄 Lead Details")
        st.caption("Browse, filter, and review enriched ENR leads." if not detail else "Detailed view for the selected lead.")
    with col2:
        st.info("Tip: Use filters on the left to narrow down leads.", icon="✨")


def sidebar_filters(data: List[Dict[str, Any]]):
    st.sidebar.header("Filters")

    firms = sorted({d.get("firm") for d in data if d.get("firm")})
    locations = sorted({d.get("location") for d in data if d.get("location")})
    countries = sorted({d.get("country") for d in data if d.get("country")})
    industries = sorted({d.get("industry") for d in data if d.get("industry")})

    # Helpers for numeric ranges (handle missing gracefully)
    ranks25 = [d.get("enr_rank_2025") for d in data if isinstance(d.get("enr_rank_2025"), (int, float))]
    r25_min, r25_max = (min(ranks25), max(ranks25)) if ranks25 else (1, 500)

    st.sidebar.text_input("Search firm", key="q")
    st.sidebar.selectbox("Location", options=["Any"] + locations, key="loc")
    st.sidebar.selectbox("Country", options=["Any"] + countries, key="country")
    st.sidebar.selectbox("Industry", options=["Any"] + industries, key="industry")
    # Guard: Streamlit slider requires min_value < max_value
    if int(r25_min) < int(r25_max):
        st.sidebar.slider("ENR Rank 2025", int(r25_min), int(r25_max), (int(r25_min), int(r25_max)), key="r25")
    else:
        st.sidebar.number_input("ENR Rank 2025", value=int(r25_min), key="r25_display", disabled=True)
        st.session_state["r25"] = (int(r25_min), int(r25_max))

    # Multi-selects from list-type fields
    all_regions = sorted({r for d in data for r in d.get("operating_regions", [])})
    all_specs = sorted({s for d in data for s in d.get("specializations", [])})
    st.sidebar.multiselect("Operating regions", options=all_regions, key="regions")
    st.sidebar.multiselect("Specializations", options=all_specs, key="specs")

    st.sidebar.divider()
    st.sidebar.selectbox("Sort by", options=["enr_rank_2025", "firm", "country"], key="sort_by")
    st.sidebar.radio("Order", options=["Ascending", "Descending"], key="sort_dir")

    st.sidebar.divider()
    st.sidebar.button("Reset filters", type="secondary", on_click=lambda: st.experimental_rerun())


def to_table_dataframe(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Build a compact DataFrame for table display and selection."""
    rows = []
    for d in data:
        rows.append({
            "Action": "View",
            "Seq": d.get("sequence"),
            "Firm": d.get("firm"),
            "Lead Score": d.get("lead_score"),
            "Completeness": d.get("completeness_score"),
            "Relevance": d.get("relevance_score"),
            "ENR 2025": d.get("enr_rank_2025"),
            "ENR 2024": d.get("enr_rank_2024"),
            "Location": d.get("location"),
            "Country": d.get("country"),
            "Industry": d.get("industry"),
            "Revenue (M)": d.get("total_revenue_m"),
        })
    df = pd.DataFrame(rows)
    # Stable sort by rank if present
    if "ENR 2025" in df.columns:
        df = df.sort_values(by=["ENR 2025", "Firm"], kind="mergesort", na_position="last")
    return df.reset_index(drop=True)


def render_table(df: pd.DataFrame) -> Tuple[int, Dict[str, Any]]:
    """Render a selectable table.
    Returns (selected_index_in_df or -1, selected_row_dict).
    """
    if AGGRID_AVAILABLE:
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(sortable=True, filter=True, resizable=True)
        # Column-specific click handler and value
        view_click = JsCode(
            """
            function(params){
                if (params && params.data && params.data.Seq !== undefined) {
                    const parentLoc = (window.parent && window.parent.location) ? window.parent.location : window.location;
                    const base = parentLoc.origin + parentLoc.pathname;
                    const url = `${base}?lead_seq=${params.data.Seq}`;
                    window.top.location.href = url;
                }
            }
            """
        )
        view_value = JsCode("function(){return 'View'}")
        gb.configure_column(
            "Action",
            valueGetter=view_value,
            onCellClicked=view_click,
            pinned='left',
            width=110,
            sortable=False,
            filter=False,
            cellStyle={"color": "#2563EB", "cursor": "pointer", "textDecoration": "underline"}
        )
        # We don't need row selection anymore
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=25)
        grid_options = gb.build()
        custom_css = {
            ".ag-header-cell-label": {"font-size": "14px", "font-weight": "600"},
            ".ag-cell": {"font-size": "14px"},
        }
        grid = AgGrid(
            df,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.NO_UPDATE,
            height=560,
            fit_columns_on_grid_load=True,
            theme="balham",  # light theme
            allow_unsafe_jscode=True,
            custom_css=custom_css,
        )
        # Navigation is handled by link; no selection to return
        return -1, {}
    else:
        st.caption("Tip: Install 'streamlit-aggrid' for new-tab action. Using fallback clickable links.")
        # Fallback: use data_editor with LinkColumn for clickable link
        df = df.copy()
        df["Action"] = df["Seq"].apply(lambda s: f"?lead_seq={int(s)}" if pd.notna(s) else "")
        st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Action": st.column_config.LinkColumn("Action", display_text="👁️ View"),
            },
            disabled=True,
        )
        return -1, {}


def apply_filters(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    q = st.session_state.get("q", "").strip().lower()
    loc = st.session_state.get("loc", "Any")
    country = st.session_state.get("country", "Any")
    industry = st.session_state.get("industry", "Any")
    r_min, r_max = st.session_state.get("r25", (1, 500))
    regions = set(st.session_state.get("regions", []))
    specs = set(st.session_state.get("specs", []))

    def match(d: Dict[str, Any]) -> bool:
        if q and q not in (d.get("firm") or "").lower():
            return False
        r25 = d.get("enr_rank_2025")
        if isinstance(r25, (int, float)):
            if not (r_min <= r25 <= r_max):
                return False
        if loc != "Any" and d.get("location") != loc:
            return False
        if country != "Any" and d.get("country") != country:
            return False
        if industry != "Any" and d.get("industry") != industry:
            return False
        if regions and not regions.issubset(set(d.get("operating_regions", []))):
            return False
        if specs and not specs.issubset(set(d.get("specializations", []))):
            return False
        return True

    filtered = [d for d in data if match(d)]

    sort_by = st.session_state.get("sort_by", "enr_rank_2025")
    reverse = st.session_state.get("sort_dir", "Ascending") == "Descending"
    return sorted(filtered, key=lambda x: (x.get(sort_by) is None, x.get(sort_by)), reverse=reverse)


def lead_card(d: Dict[str, Any]):
    with st.container(border=True):
        top_l, top_r = st.columns([0.75, 0.25])
        with top_l:
            st.subheader(d.get("firm", "Unknown"))
            meta = []
            if d.get("location"): meta.append(d["location"])
            if d.get("country"): meta.append(d["country"])
            if d.get("industry"): meta.append(d["industry"])
            st.caption(" • ".join(meta))
        with top_r:
            c1, c2 = st.columns(2)
            with c1:
                st.metric("ENR 2025", d.get("enr_rank_2025", "—"))
            with c2:
                st.metric("ENR 2024", d.get("enr_rank_2024", "—"))

        # Scores row
        s1, s2, s3 = st.columns(3)
        with s1:
            st.metric("Lead Score", d.get("lead_score", "—"))
        with s2:
            comp = d.get("completeness_score")
            st.metric("Completeness", f"{comp:.2f}" if isinstance(comp, (int, float)) else "—")
        with s3:
            rel = d.get("relevance_score")
            st.metric("Relevance", f"{rel:.2f}" if isinstance(rel, (int, float)) else "—")

        st.write(d.get("description") or "")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Revenue (M)", d.get("total_revenue_m", "—"))
        with c2:
            st.metric("Intl Revenue (M)", d.get("int_total_revenue_m", "—"))
        with c3:
            st.metric("New Contracts (M)", d.get("new_contracts", "—"))
        with c4:
            st.metric("General Building %", d.get("general_building_pct", "—"))

        if d.get("website"):
            st.link_button("Visit Website", f"https://{d['website']}" if not d["website"].startswith("http") else d["website"], type="primary")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Operating Regions**")
            if d.get("operating_regions"):
                for r in d["operating_regions"]:
                    tagline(r)
            else:
                st.caption("No regions listed")
        with col_b:
            st.markdown("**Specializations**")
            if d.get("specializations"):
                for s in d["specializations"]:
                    tagline(s)
            else:
                st.caption("No specializations listed")

        with st.expander("Contacts and People"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Key People**")
                for p in d.get("key_people", []):
                    st.write(f"- {p.get('name', '—')} — {p.get('position', '')}")
                if not d.get("key_people"): st.caption("No key people listed")

                st.markdown("**Emails**")
                for e in d.get("contact_emails", []):
                    st.write(f"- {e.get('contact_name', '—')}: {e.get('email', '')} ({e.get('designation','')})")
                if not d.get("contact_emails"): st.caption("No emails listed")

            with c2:
                st.markdown("**Phones**")
                for p in d.get("contact_phones", []):
                    st.write(f"- {p.get('contact_name', '—')}: {p.get('phone', '')} ({p.get('designation','')})")
                if not d.get("contact_phones"): st.caption("No phones listed")

                st.markdown("**LinkedIns**")
                for l in d.get("contact_linkedins", []):
                    url = l.get("linkedin_url", "")
                    name = l.get("contact_name", "Profile")
                    st.write(f"- [{name}]({url})")
                if not d.get("contact_linkedins"): st.caption("No LinkedIn links listed")


def main():
    # Determine mode from query params
    params = st.experimental_get_query_params()
    lead_seq = None
    if "lead_seq" in params:
        try:
            lead_seq = int(params["lead_seq"][0])
        except Exception:
            lead_seq = None

    data = load_data()

    if lead_seq is not None:
        # Detail page
        inject_styles()
        render_header(detail=True)
        # Find by sequence
        rec = None
        for r in data:
            if r.get("sequence") == lead_seq:
                rec = r
                break
        if rec is None:
            st.error("Lead not found.")
            st.markdown("[← Back to list](?)")
            return
        st.markdown("[← Back to list](?)")
        lead_card(rec)
        return

    # List page
    inject_styles()
    render_header(detail=False)
    sidebar_filters(data)

    results = apply_filters(data)
    st.write(f"Showing {len(results)} of {len(data)} leads")

    df = to_table_dataframe(results)
    render_table(df)


if __name__ == "__main__":
    main()