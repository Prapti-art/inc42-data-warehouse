"""
Inc42 Data Warehouse — Executive Dashboard (Optimized)
All queries run server-side on BigQuery — no full table loads into memory.

Run locally: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import hmac

# ── Config ──
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json")
BQ_PROJECT = "bigquery-296406"

st.set_page_config(
    page_title="Inc42 Data Warehouse",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ══════════════════════════════════════════════
#  AUTHENTICATION
# ══════════════════════════════════════════════
def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if st.session_state.authenticated:
        return True

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.markdown("### Inc42 Data Warehouse")
        password = st.text_input("Password", type="password", key="pwd_input")
        if st.button("Login", use_container_width=True):
            correct_password = os.environ.get("DASHBOARD_PASSWORD", "inc42data2026")
            if hmac.compare_digest(password, correct_password):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")
    return False

if not check_password():
    st.stop()


# ══════════════════════════════════════════════
#  QUERY HELPER (cached, server-side)
# ══════════════════════════════════════════════
@st.cache_data(ttl=300)
def q(sql):
    """Run BigQuery SQL and return DataFrame. Results cached 5 minutes."""
    bq = bigquery.Client(project=BQ_PROJECT)
    return bq.query(sql).to_dataframe()


# ── Sidebar ──
st.sidebar.title("Inc42 Data Warehouse")
st.sidebar.caption("Refreshed daily at midnight IST")

page = st.sidebar.radio("", [
    "Executive Overview",
    "Revenue & Products",
    "Newsletter & Marketing",
    "Events & Programs",
    "Lead Discovery",
    "Contact Lookup",
    "Company Intelligence",
])

st.sidebar.divider()
if st.sidebar.button("Logout"):
    st.session_state.authenticated = False
    st.rerun()


# ══════════════════════════════════════════════
#  PAGE 1: EXECUTIVE OVERVIEW
# ══════════════════════════════════════════════
if page == "Executive Overview":
    st.title("Executive Overview")

    stats = q("""
        SELECT
            COUNT(*) AS total_contacts,
            COUNTIF(total_orders > 0) AS paying_contacts,
            COUNTIF(total_newsletters_subscribed > 0) AS newsletter_subs,
            ROUND(SUM(total_revenue), 0) AS total_revenue,
            ROUND(AVG(engagement_score), 1) AS avg_engagement,
            COUNTIF(email_reachability = 'reachable') AS reachable,
            COUNTIF(company_is_unicorn) AS unicorn_contacts,
            COUNTIF(company_is_soonicorn) AS soonicorn_contacts,
            COUNTIF(company_is_fast42_winner) AS fast42_contacts,
            COUNTIF(is_premier_institute_alumni) AS premier_alumni,
            COUNTIF(is_investor) AS investors
        FROM silver_gold.contact_360
    """)
    s = stats.iloc[0]

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Contacts", f"{s['total_contacts']:,}")
    c2.metric("Paying Customers", f"{s['paying_contacts']:,}")
    c3.metric("Newsletter Subs", f"{s['newsletter_subs']:,}")
    c4.metric("Total Revenue", f"₹{s['total_revenue']:,.0f}")
    c5.metric("Avg Engagement", f"{s['avg_engagement']}")
    c6.metric("Email Reachable", f"{s['reachable']:,}")

    st.divider()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Unicorn Contacts", f"{s['unicorn_contacts']:,}")
    c2.metric("Soonicorn Contacts", f"{s['soonicorn_contacts']:,}")
    c3.metric("FAST42 Contacts", f"{s['fast42_contacts']:,}")
    c4.metric("Premier Alumni", f"{s['premier_alumni']:,}")
    c5.metric("Investors", f"{s['investors']:,}")

    st.divider()
    col_left, col_mid, col_right = st.columns(3)

    with col_left:
        st.subheader("Contacts by Source Count")
        st.bar_chart(q("SELECT source_count, COUNT(*) AS contacts FROM silver_gold.contact_360 GROUP BY source_count ORDER BY source_count").set_index("source_count"))

    with col_mid:
        st.subheader("Email Reachability")
        st.bar_chart(q("SELECT email_reachability, COUNT(*) AS contacts FROM silver_gold.contact_360 GROUP BY email_reachability").set_index("email_reachability"))

    with col_right:
        st.subheader("Top 15 Cities")
        st.bar_chart(q("SELECT city, COUNT(*) AS contacts FROM silver_gold.contact_360 WHERE city IS NOT NULL AND city != '' GROUP BY city ORDER BY contacts DESC LIMIT 15").set_index("city"))

    st.subheader("Engagement Score Distribution")
    st.bar_chart(q("""
        SELECT
            CASE
                WHEN engagement_score = 0 THEN '0 Inactive'
                WHEN engagement_score < 10 THEN '1-9 Low'
                WHEN engagement_score < 50 THEN '10-49 Medium'
                WHEN engagement_score < 100 THEN '50-99 High'
                ELSE '100+ Super'
            END AS tier, COUNT(*) AS contacts
        FROM silver_gold.contact_360
        GROUP BY tier ORDER BY MIN(engagement_score)
    """).set_index("tier"))

    st.subheader("Top Company Sectors")
    st.bar_chart(q("""
        SELECT sector, COUNT(*) AS contacts
        FROM silver_gold.contact_360, UNNEST(SPLIT(company_sector, ', ')) AS sector
        WHERE company_sector IS NOT NULL
        GROUP BY sector ORDER BY contacts DESC LIMIT 15
    """).set_index("sector"))


# ══════════════════════════════════════════════
#  PAGE 2: REVENUE & PRODUCTS
# ══════════════════════════════════════════════
elif page == "Revenue & Products":
    st.title("Revenue & Products")

    stats = q("""
        SELECT COUNT(*) AS total_orders,
            COUNTIF(is_completed=1) AS completed, COUNTIF(is_refunded=1) AS refunded,
            COUNTIF(is_cancelled=1) AS cancelled,
            ROUND(SUM(CASE WHEN is_completed=1 THEN net_revenue ELSE 0 END),0) AS revenue,
            ROUND(AVG(CASE WHEN is_completed=1 THEN net_revenue END),0) AS avg_order
        FROM silver_gold.fact_orders
    """).iloc[0]

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Total Orders", f"{stats['total_orders']:,}")
    c2.metric("Completed", f"{stats['completed']:,}")
    c3.metric("Revenue", f"₹{stats['revenue']:,.0f}")
    c4.metric("Refunded", f"{stats['refunded']:,}")
    c5.metric("Cancelled", f"{stats['cancelled']:,}")
    c6.metric("Avg Order", f"₹{stats['avg_order']:,.0f}" if pd.notna(stats['avg_order']) else "N/A")

    st.divider()
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Revenue by Product")
        st.bar_chart(q("SELECT product_name, ROUND(SUM(CASE WHEN is_completed=1 THEN net_revenue ELSE 0 END),0) AS revenue FROM silver_gold.fact_orders GROUP BY product_name ORDER BY revenue DESC").set_index("product_name"))
    with col_right:
        st.subheader("Orders by Duration")
        st.bar_chart(q("SELECT membership_duration, COUNT(*) AS orders FROM silver_gold.fact_orders WHERE membership_duration IS NOT NULL GROUP BY membership_duration ORDER BY orders DESC").set_index("membership_duration"))

    st.subheader("Orders by Status")
    st.bar_chart(q("SELECT order_status, COUNT(*) AS orders FROM silver_gold.fact_orders GROUP BY order_status ORDER BY orders DESC").set_index("order_status"))

    st.subheader("Top 20 Spenders")
    st.dataframe(q("""
        SELECT email, CONCAT(first_name, ' ', last_name) AS name, company_name,
               total_orders, ROUND(total_revenue,0) AS total_revenue, company_sector
        FROM silver_gold.contact_360 WHERE total_revenue > 0
        ORDER BY total_revenue DESC LIMIT 20
    """), use_container_width=True)

    st.subheader("Product Performance Detail")
    st.dataframe(q("""
        SELECT product_name, product_type, membership_duration,
            COUNT(*) AS orders, COUNTIF(is_completed=1) AS completed,
            ROUND(SUM(CASE WHEN is_completed=1 THEN net_revenue ELSE 0 END),0) AS revenue,
            COUNTIF(is_refunded=1) AS refunded, COUNTIF(is_cancelled=1) AS cancelled
        FROM silver_gold.fact_orders
        GROUP BY product_name, product_type, membership_duration ORDER BY orders DESC
    """), use_container_width=True)


# ══════════════════════════════════════════════
#  PAGE 3: NEWSLETTER & MARKETING
# ══════════════════════════════════════════════
elif page == "Newsletter & Marketing":
    st.title("Newsletter & Marketing")

    nl = q("""
        SELECT
            COUNTIF(daily_newsletter='subscribed') AS daily,
            COUNTIF(weekly_newsletter='subscribed') AS weekly,
            COUNTIF(indepth_newsletter='subscribed') AS indepth,
            COUNTIF(moneyball_newsletter='subscribed') AS moneyball,
            COUNTIF(theoutline_newsletter='subscribed') AS theoutline,
            COUNTIF(markets_newsletter='subscribed') AS markets,
            COUNTIF(whatsapp_optin='Yes') AS whatsapp,
            COUNTIF(is_globally_unsubscribed) AS unsubscribed
        FROM silver_gold.contact_360
    """).iloc[0]

    st.subheader("Newsletter Subscribers")
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Daily", f"{nl['daily']:,}")
    c2.metric("Weekly", f"{nl['weekly']:,}")
    c3.metric("InDepth", f"{nl['indepth']:,}")
    c4.metric("Moneyball", f"{nl['moneyball']:,}")
    c5.metric("TheOutline", f"{nl['theoutline']:,}")
    c6.metric("Markets", f"{nl['markets']:,}")

    st.bar_chart(q("""
        SELECT 'Daily' AS newsletter, COUNTIF(daily_newsletter='subscribed') AS subscribers FROM silver_gold.contact_360
        UNION ALL SELECT 'Weekly', COUNTIF(weekly_newsletter='subscribed') FROM silver_gold.contact_360
        UNION ALL SELECT 'InDepth', COUNTIF(indepth_newsletter='subscribed') FROM silver_gold.contact_360
        UNION ALL SELECT 'Moneyball', COUNTIF(moneyball_newsletter='subscribed') FROM silver_gold.contact_360
        UNION ALL SELECT 'TheOutline', COUNTIF(theoutline_newsletter='subscribed') FROM silver_gold.contact_360
        UNION ALL SELECT 'Markets', COUNTIF(markets_newsletter='subscribed') FROM silver_gold.contact_360
    """).set_index("newsletter"))

    st.divider()
    c1,c2 = st.columns(2)
    with c1:
        st.subheader("Newsletters per Person")
        st.bar_chart(q("SELECT total_newsletters_subscribed, COUNT(*) AS people FROM silver_gold.contact_360 GROUP BY total_newsletters_subscribed ORDER BY total_newsletters_subscribed").set_index("total_newsletters_subscribed"))
    with c2:
        st.subheader("Reachability")
        c1,c2,c3 = st.columns(3)
        c1.metric("WhatsApp Opted In", f"{nl['whatsapp']:,}")
        c2.metric("Globally Unsubscribed", f"{nl['unsubscribed']:,}")

    st.divider()
    st.subheader("Email Marketing Performance")
    mkt = q("""
        SELECT SUM(sent) AS sent, SUM(delivered) AS delivered, SUM(opened) AS opened,
               SUM(clicked) AS clicked, SUM(bounced) AS bounced
        FROM silver_gold.fact_marketing_touchpoints
    """).iloc[0]
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Sent", f"{mkt['sent']:,}")
    c2.metric("Delivered", f"{mkt['delivered']:,}")
    c3.metric("Open Rate", f"{(mkt['opened']/mkt['delivered']*100) if mkt['delivered']>0 else 0:.1f}%")
    c4.metric("Click Rate", f"{(mkt['clicked']/mkt['delivered']*100) if mkt['delivered']>0 else 0:.1f}%")
    c5.metric("Bounce Rate", f"{(mkt['bounced']/mkt['sent']*100) if mkt['sent']>0 else 0:.1f}%")

    st.divider()
    st.subheader("Newsletter → Paying Customer Conversion")
    st.dataframe(q("""
        SELECT newsletter, subscribers, paying, ROUND(paying/subscribers*100,1) AS conversion_pct FROM (
            SELECT 'Daily' AS newsletter, COUNTIF(daily_newsletter='subscribed') AS subscribers, COUNTIF(daily_newsletter='subscribed' AND total_orders>0) AS paying FROM silver_gold.contact_360
            UNION ALL SELECT 'Weekly', COUNTIF(weekly_newsletter='subscribed'), COUNTIF(weekly_newsletter='subscribed' AND total_orders>0) FROM silver_gold.contact_360
            UNION ALL SELECT 'Moneyball', COUNTIF(moneyball_newsletter='subscribed'), COUNTIF(moneyball_newsletter='subscribed' AND total_orders>0) FROM silver_gold.contact_360
            UNION ALL SELECT 'InDepth', COUNTIF(indepth_newsletter='subscribed'), COUNTIF(indepth_newsletter='subscribed' AND total_orders>0) FROM silver_gold.contact_360
            UNION ALL SELECT 'TheOutline', COUNTIF(theoutline_newsletter='subscribed'), COUNTIF(theoutline_newsletter='subscribed' AND total_orders>0) FROM silver_gold.contact_360
        )
    """), use_container_width=True)


# ══════════════════════════════════════════════
#  PAGE 4: EVENTS & PROGRAMS
# ══════════════════════════════════════════════
elif page == "Events & Programs":
    st.title("Events & Programs")

    st.subheader("Submissions by Category")
    cat = q("SELECT form_category, COUNT(*) AS submissions FROM silver_gold.fact_form_submissions GROUP BY form_category ORDER BY submissions DESC")
    c1,c2,c3,c4,c5 = st.columns(5)
    for i, row in cat.head(5).iterrows():
        [c1,c2,c3,c4,c5][i].metric(row['form_category'], f"{row['submissions']:,}")
    st.bar_chart(cat.set_index("form_category"))

    st.divider()
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("FAST42 by Edition")
        st.dataframe(q("SELECT edition, COUNT(*) AS applications FROM silver_gold.fact_form_submissions WHERE form_category='FAST42' GROUP BY edition ORDER BY applications DESC"), use_container_width=True)
    with col_right:
        st.subheader("D2C Events")
        st.dataframe(q("SELECT form_subcategory, edition, COUNT(*) AS registrations FROM silver_gold.fact_form_submissions WHERE form_category='D2C Event' GROUP BY form_subcategory, edition ORDER BY registrations DESC"), use_container_width=True)

    st.divider()
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("AI Workshop by Audience")
        st.bar_chart(q("SELECT form_subcategory, COUNT(*) AS signups FROM silver_gold.fact_form_submissions WHERE form_category='AI Workshop' GROUP BY form_subcategory ORDER BY signups DESC").set_index("form_subcategory"))
    with col_right:
        st.subheader("GenAI Summit")
        st.dataframe(q("SELECT form_subcategory, edition, COUNT(*) AS count FROM silver_gold.fact_form_submissions WHERE form_category='GenAI Summit' GROUP BY form_subcategory, edition"), use_container_width=True)

    st.subheader("Report Downloads by Type")
    st.dataframe(q("SELECT form_subcategory, edition, COUNT(*) AS downloads FROM silver_gold.fact_form_submissions WHERE form_category='Report Download' GROUP BY form_subcategory, edition ORDER BY downloads DESC"), use_container_width=True)

    st.subheader("Full Form Breakdown")
    form_detail = q("SELECT form_category, form_subcategory, edition, source_system, COUNT(*) AS submissions FROM silver_gold.fact_form_submissions GROUP BY 1,2,3,4 ORDER BY submissions DESC")
    st.dataframe(form_detail, use_container_width=True, height=400)
    st.download_button("Download Form Data", form_detail.to_csv(index=False), "form_submissions.csv", "text/csv")


# ══════════════════════════════════════════════
#  PAGE 5: LEAD DISCOVERY
# ══════════════════════════════════════════════
elif page == "Lead Discovery":
    st.title("Lead Discovery")
    st.caption("Find and export contact lists based on filters")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        sector_filter = st.text_input("Company Sector (e.g. Fintech, SaaS)")
    with col2:
        city_filter = st.text_input("City (e.g. Bengaluru, Mumbai)")
    with col3:
        min_engagement = st.slider("Min Engagement Score", 0, 500, 0)
    with col4:
        min_sources = st.slider("Min Source Count", 1, 6, 1)

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        has_orders = st.checkbox("Has orders")
    with col6:
        email_reachable = st.checkbox("Email reachable only")
    with col7:
        unicorn_only = st.checkbox("Unicorn company")
    with col8:
        premier_alumni = st.checkbox("Premier institute alumni")

    # Build WHERE clause
    conditions = [f"engagement_score >= {min_engagement}", f"source_count >= {min_sources}"]
    if sector_filter:
        conditions.append(f"LOWER(company_sector) LIKE '%{sector_filter.lower()}%'")
    if city_filter:
        conditions.append(f"LOWER(city) LIKE '%{city_filter.lower()}%'")
    if has_orders:
        conditions.append("total_orders > 0")
    if email_reachable:
        conditions.append("email_reachability = 'reachable'")
    if unicorn_only:
        conditions.append("company_is_unicorn = TRUE")
    if premier_alumni:
        conditions.append("is_premier_institute_alumni = TRUE")

    where = " AND ".join(conditions)

    # Count first (fast)
    count = q(f"SELECT COUNT(*) AS n FROM silver_gold.contact_360 WHERE {where}").iloc[0]['n']
    st.subheader(f"Results: {count:,} contacts")

    if count > 0:
        results = q(f"""
            SELECT email, first_name, last_name, company_name, designation, seniority,
                   city, phone, company_sector, engagement_score, total_orders,
                   ROUND(total_revenue,0) AS total_revenue, total_newsletters_subscribed,
                   ROUND(company_total_funding_usd,0) AS company_funding,
                   company_is_unicorn, source_count, email_reachability
            FROM silver_gold.contact_360
            WHERE {where}
            ORDER BY engagement_score DESC
            LIMIT 1000
        """)

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Showing", f"{len(results):,}")
        c2.metric("With Phone", f"{results['phone'].notna().sum():,}")
        c3.metric("Avg Engagement", f"{results['engagement_score'].mean():.1f}")
        c4.metric("Total Revenue", f"₹{results['total_revenue'].sum():,.0f}")

        st.dataframe(results, use_container_width=True, height=500)

        # Full export with all columns
        if st.button("Generate Full Export CSV"):
            full = q(f"""
                SELECT email, first_name, last_name, phone, company_name, designation,
                       seniority, city, state, country, linkedin_url, company_sector,
                       company_sub_sectors, company_business_model, company_total_funding_usd,
                       company_last_funding_stage, company_employee_count, company_investors,
                       company_is_unicorn, company_inc42_tags, dl_education_institution,
                       is_premier_institute_alumni, engagement_score, total_orders, total_revenue,
                       total_form_submissions, total_events_registered, total_emails_opened,
                       daily_newsletter, weekly_newsletter, moneyball_newsletter,
                       total_newsletters_subscribed, whatsapp_optin, email_reachability,
                       source_count, found_in_systems, all_emails
                FROM silver_gold.contact_360
                WHERE {where}
                ORDER BY engagement_score DESC
            """)
            st.download_button(f"Download {len(full):,} contacts", full.to_csv(index=False), "inc42_leads.csv", "text/csv")


# ══════════════════════════════════════════════
#  PAGE 6: CONTACT LOOKUP
# ══════════════════════════════════════════════
elif page == "Contact Lookup":
    st.title("Contact Lookup — Full 360 View")

    search = st.text_input("Search by email, name, or company (min 3 characters)")

    if search and len(search) >= 3:
        search_safe = search.replace("'", "\\'").lower()
        results = q(f"""
            SELECT * FROM silver_gold.contact_360
            WHERE LOWER(email) LIKE '%{search_safe}%'
               OR LOWER(CONCAT(COALESCE(first_name,''), ' ', COALESCE(last_name,''))) LIKE '%{search_safe}%'
               OR LOWER(company_name) LIKE '%{search_safe}%'
            ORDER BY engagement_score DESC LIMIT 50
        """)

        if results.empty:
            st.warning("No contacts found")
        else:
            st.write(f"Found **{len(results)}** contacts")

            for _, p in results.head(20).iterrows():
                name = f"{p.get('first_name','') or ''} {p.get('last_name','') or ''}".strip() or p['email']
                company = p.get('company_name','') or 'No company'

                with st.expander(f"{name} — {company} (Score: {p.get('engagement_score',0):.0f})"):
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.markdown("**Identity**")
                        st.write(f"Email: {p['email']}")
                        st.write(f"Phone: {p.get('phone') or 'N/A'}")
                        st.write(f"City: {p.get('city') or 'N/A'}")
                        if p.get('linkedin_url'): st.write(f"LinkedIn: {p['linkedin_url']}")
                        st.write(f"Sources ({p['source_count']}): {p.get('found_in_systems','')}")

                    with col2:
                        st.markdown("**Professional**")
                        st.write(f"Designation: {p.get('designation') or 'N/A'}")
                        st.write(f"Seniority: {p.get('seniority') or 'N/A'}")
                        if p.get('dl_current_company'): st.write(f"DL Company: {p['dl_current_company']}")
                        if p.get('dl_education_institution'):
                            st.write(f"Education: {p['dl_education_institution']}")
                            if p.get('dl_education_degree'): st.write(f"Degree: {p['dl_education_degree']}")
                        if p.get('is_premier_institute_alumni'):
                            badges = []
                            for flag, label in [('is_iit_alumni','IIT'),('is_iim_alumni','IIM'),('is_isb_alumni','ISB'),('is_bits_alumni','BITS'),('is_global_top_alumni','Global Top')]:
                                if p.get(flag): badges.append(label)
                            if badges: st.write(f"Alumni: {', '.join(badges)}")

                    with col3:
                        st.markdown("**Company Intel**")
                        st.write(f"Sector: {p.get('company_sector') or 'N/A'}")
                        if p.get('company_total_funding_usd') and float(p.get('company_total_funding_usd',0) or 0) > 0:
                            st.write(f"Funding: ${float(p['company_total_funding_usd']):,.0f}")
                            st.write(f"Stage: {p.get('company_last_funding_stage') or 'N/A'}")
                        if p.get('company_employee_count'):
                            st.write(f"Employees: {float(p['company_employee_count']):,.0f}")
                        if p.get('company_investors'):
                            st.write(f"Investors: {str(p['company_investors'])[:120]}")
                        if p.get('company_inc42_tags'):
                            st.write(f"Tags: {p['company_inc42_tags']}")
                        flags = []
                        if p.get('company_is_unicorn'): flags.append("Unicorn")
                        if p.get('company_is_soonicorn'): flags.append("Soonicorn")
                        if p.get('company_is_fast42_winner'): flags.append("FAST42")
                        if flags: st.write(f"Status: {', '.join(flags)}")

                    with col4:
                        st.markdown("**Engagement**")
                        st.write(f"Orders: {p['total_orders']} (₹{float(p['total_revenue']):,.0f})")
                        st.write(f"Forms: {p['total_form_submissions']}")
                        st.write(f"Events: {p['total_events_registered']}")
                        st.write(f"Emails Opened: {p['total_emails_opened']}")
                        st.write(f"Newsletters: {p['total_newsletters_subscribed']}")
                        st.write(f"Reachability: {p.get('email_reachability','N/A')}")

    elif search:
        st.info("Type at least 3 characters")
    else:
        st.info("Search for any contact by email, name, or company")


# ══════════════════════════════════════════════
#  PAGE 7: COMPANY INTELLIGENCE
# ══════════════════════════════════════════════
elif page == "Company Intelligence":
    st.title("Company Intelligence")

    stats = q("""
        SELECT COUNT(*) AS total, COUNTIF(total_funding_usd>0) AS funded,
               COUNTIF(is_profitable) AS profitable, COUNTIF(is_unicorn) AS unicorns,
               COUNTIF(is_soonicorn) AS soonicorns, COUNTIF(is_fast42_winner) AS fast42
        FROM silver_gold.company_360
    """).iloc[0]

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Total Companies", f"{stats['total']:,}")
    c2.metric("With Funding", f"{stats['funded']:,}")
    c3.metric("Profitable", f"{stats['profitable']:,}")
    c4.metric("Unicorns", f"{stats['unicorns']:,}")
    c5.metric("Soonicorns", f"{stats['soonicorns']:,}")
    c6.metric("FAST42", f"{stats['fast42']:,}")

    st.divider()
    col1, col2, col3 = st.columns(3)
    with col1:
        sector_filter = st.text_input("Filter by sector")
    with col2:
        min_funding = st.number_input("Min Funding (USD)", value=0, step=1000000)
    with col3:
        tag_filter = st.selectbox("Inc42 Tag", ["All", "Unicorn", "Soonicorn", "Minicorn", "FAST42", "30 Startups To Watch"])

    conditions = ["1=1"]
    if sector_filter:
        conditions.append(f"LOWER(sector) LIKE '%{sector_filter.lower()}%'")
    if min_funding > 0:
        conditions.append(f"total_funding_usd >= {min_funding}")
    if tag_filter != "All":
        tag_col = {"Unicorn": "is_unicorn", "Soonicorn": "is_soonicorn", "Minicorn": "is_minicorn", "FAST42": "is_fast42_winner", "30 Startups To Watch": "is_30_startups_to_watch"}[tag_filter]
        conditions.append(f"{tag_col} = TRUE")

    where = " AND ".join(conditions)
    count = q(f"SELECT COUNT(*) AS n FROM silver_gold.company_360 WHERE {where}").iloc[0]['n']
    st.subheader(f"Companies: {count:,}")

    st.dataframe(q(f"""
        SELECT company_name, sector, sub_sectors, city, founded_year,
               total_funding_usd, last_funding_stage, investor_count,
               employee_count, is_profitable, monthly_visits,
               inc42_tags, contacts_in_warehouse, total_revenue_from_company
        FROM silver_gold.company_360
        WHERE {where}
        ORDER BY total_funding_usd DESC LIMIT 500
    """), use_container_width=True, height=500)

    st.divider()
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Top Sectors by Funding")
        st.bar_chart(q(f"""
            SELECT sector, ROUND(SUM(total_funding_usd),0) AS funding
            FROM silver_gold.company_360, UNNEST(SPLIT(sector, ', ')) AS sector
            WHERE sector IS NOT NULL AND {where}
            GROUP BY sector ORDER BY funding DESC LIMIT 15
        """).set_index("sector"))
    with col_right:
        st.subheader("Top Companies with Inc42 Contacts")
        st.dataframe(q(f"""
            SELECT company_name, sector, contacts_in_warehouse, total_revenue_from_company
            FROM silver_gold.company_360 WHERE contacts_in_warehouse > 0 AND {where}
            ORDER BY contacts_in_warehouse DESC LIMIT 15
        """), use_container_width=True)


# ── Footer ──
st.sidebar.divider()
st.sidebar.caption("v2.1 — Inc42 Data Warehouse")
st.sidebar.caption("330K contacts | 75K companies | 25M+ raw rows")
