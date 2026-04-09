"""
Inc42 Data Warehouse — Streamlit Dashboard
Reads from BigQuery Gold layer (contact_360, company_360)

Run: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json")

BQ_PROJECT = "bigquery-296406"

st.set_page_config(
    page_title="Inc42 Data Warehouse",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=300)
def load_data(query):
    """Load data from BigQuery with 5-minute cache."""
    bq = bigquery.Client(project=BQ_PROJECT)
    return bq.query(query).to_dataframe()


# ── Sidebar ──
st.sidebar.title("Inc42 Data Warehouse")
page = st.sidebar.radio("Navigate", [
    "Overview",
    "Contact Explorer",
    "Company Explorer",
    "Revenue & Orders",
    "Event Intelligence",
    "Marketing & Engagement",
    "Contact Lookup",
])


# ═══════════════════════════════════════════
#  OVERVIEW
# ═══════════════════════════════════════════
if page == "Overview":
    st.title("Inc42 Data Warehouse — Overview")

    # Key metrics
    stats = load_data("""
        SELECT
            COUNT(*) AS total_contacts,
            COUNTIF(total_orders > 0) AS paying_contacts,
            COUNTIF(source_count >= 2) AS multi_source_contacts,
            ROUND(SUM(total_revenue), 0) AS total_revenue,
            SUM(total_orders) AS total_orders,
            SUM(total_form_submissions) AS total_form_submissions,
            SUM(total_events_registered) AS total_events,
            ROUND(AVG(engagement_score), 1) AS avg_engagement
        FROM silver_gold.contact_360
    """)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Contacts", f"{stats['total_contacts'].iloc[0]:,}")
    col2.metric("Paying Contacts", f"{stats['paying_contacts'].iloc[0]:,}")
    col3.metric("Multi-Source", f"{stats['multi_source_contacts'].iloc[0]:,}")
    col4.metric("Avg Engagement", f"{stats['avg_engagement'].iloc[0]}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Total Revenue", f"₹{stats['total_revenue'].iloc[0]:,.0f}")
    col6.metric("Total Orders", f"{stats['total_orders'].iloc[0]:,}")
    col7.metric("Form Submissions", f"{stats['total_form_submissions'].iloc[0]:,}")
    col8.metric("Event Registrations", f"{stats['total_events'].iloc[0]:,}")

    st.divider()

    # Source distribution
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Contacts by Source Count")
        source_dist = load_data("""
            SELECT source_count, COUNT(*) AS contacts
            FROM silver_gold.contact_360
            GROUP BY source_count ORDER BY source_count
        """)
        st.bar_chart(source_dist.set_index("source_count"))

    with col_right:
        st.subheader("Top 15 Cities")
        cities = load_data("""
            SELECT COALESCE(city, 'Unknown') AS city, COUNT(*) AS contacts
            FROM silver_gold.contact_360
            WHERE city IS NOT NULL AND city != ''
            GROUP BY city ORDER BY contacts DESC LIMIT 15
        """)
        st.bar_chart(cities.set_index("city"))

    # Companies overview
    st.divider()
    st.subheader("Company Database")
    company_stats = load_data("""
        SELECT
            COUNT(*) AS total_companies,
            COUNTIF(total_funding_usd > 0) AS funded_companies,
            COUNTIF(is_profitable = TRUE) AS profitable_companies,
            COUNTIF(employee_count > 0) AS with_employee_data,
            ROUND(SUM(total_funding_usd) / 1e9, 2) AS total_funding_bn_usd
        FROM silver_gold.company_360
    """)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Companies", f"{company_stats['total_companies'].iloc[0]:,}")
    c2.metric("Funded", f"{company_stats['funded_companies'].iloc[0]:,}")
    c3.metric("Profitable", f"{company_stats['profitable_companies'].iloc[0]:,}")
    c4.metric("With Employees", f"{company_stats['with_employee_data'].iloc[0]:,}")
    c5.metric("Total Funding", f"${company_stats['total_funding_bn_usd'].iloc[0]}B")


# ═══════════════════════════════════════════
#  CONTACT EXPLORER
# ═══════════════════════════════════════════
elif page == "Contact Explorer":
    st.title("Contact Explorer")

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        min_sources = st.slider("Min source count", 1, 6, 1)
    with col2:
        min_engagement = st.slider("Min engagement score", 0, 500, 0)
    with col3:
        has_orders = st.checkbox("Has orders only")

    order_filter = "AND total_orders > 0" if has_orders else ""

    contacts = load_data(f"""
        SELECT email, full_name, company_name, city, designation,
               source_count, engagement_score, total_orders, total_revenue,
               total_form_submissions, total_events_registered,
               total_emails_opened, found_in_systems
        FROM silver_gold.contact_360
        WHERE source_count >= {min_sources}
          AND engagement_score >= {min_engagement}
          {order_filter}
        ORDER BY engagement_score DESC
        LIMIT 1000
    """)

    st.write(f"Showing {len(contacts):,} contacts (top 1000 by engagement)")
    st.dataframe(contacts, use_container_width=True, height=600)

    # Download
    csv = contacts.to_csv(index=False)
    st.download_button("Download CSV", csv, "contacts.csv", "text/csv")


# ═══════════════════════════════════════════
#  COMPANY EXPLORER
# ═══════════════════════════════════════════
elif page == "Company Explorer":
    st.title("Company Explorer")

    col1, col2 = st.columns(2)
    with col1:
        sector_filter = st.text_input("Filter by sector (e.g. Fintech, SaaS)")
    with col2:
        min_funding = st.number_input("Min funding (USD)", value=0, step=100000)

    sector_clause = f"AND LOWER(sector) LIKE '%{sector_filter.lower()}%'" if sector_filter else ""

    companies = load_data(f"""
        SELECT company_name, sector, sub_sector, city, founded_year,
               total_funding_usd, last_funding_stage, employee_count,
               is_profitable, monthly_visits,
               contacts_in_warehouse, total_revenue_from_company
        FROM silver_gold.company_360
        WHERE total_funding_usd >= {min_funding}
          {sector_clause}
        ORDER BY total_funding_usd DESC
        LIMIT 1000
    """)

    st.write(f"Showing {len(companies):,} companies")
    st.dataframe(companies, use_container_width=True, height=600)

    csv = companies.to_csv(index=False)
    st.download_button("Download CSV", csv, "companies.csv", "text/csv")


# ═══════════════════════════════════════════
#  REVENUE & ORDERS
# ═══════════════════════════════════════════
elif page == "Revenue & Orders":
    st.title("Revenue & Orders")

    # Summary metrics
    order_stats = load_data("""
        SELECT
            COUNT(*) AS total_orders,
            COUNTIF(is_completed = 1) AS completed,
            COUNTIF(is_refunded = 1) AS refunded,
            COUNTIF(is_cancelled = 1) AS cancelled,
            ROUND(SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END), 0) AS completed_revenue,
            ROUND(AVG(CASE WHEN is_completed = 1 THEN net_revenue END), 0) AS avg_order_value
        FROM silver_gold.fact_orders
    """)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Orders", f"{order_stats['total_orders'].iloc[0]:,}")
    c2.metric("Completed", f"{order_stats['completed'].iloc[0]:,}")
    c3.metric("Refunded", f"{order_stats['refunded'].iloc[0]:,}")
    c4.metric("Cancelled", f"{order_stats['cancelled'].iloc[0]:,}")
    c5.metric("Revenue", f"₹{order_stats['completed_revenue'].iloc[0]:,.0f}")
    c6.metric("Avg Order", f"₹{order_stats['avg_order_value'].iloc[0]:,.0f}")

    st.divider()

    # Orders by status
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Orders by Status")
        by_status = load_data("""
            SELECT order_status, COUNT(*) AS orders, ROUND(SUM(net_revenue), 0) AS revenue
            FROM silver_gold.fact_orders
            GROUP BY order_status ORDER BY orders DESC
        """)
        st.dataframe(by_status, use_container_width=True)

    with col_right:
        st.subheader("Top 10 Spenders")
        top_spenders = load_data("""
            SELECT c.email, c.full_name, c.company_name,
                   c.total_orders, ROUND(c.total_revenue, 0) AS total_revenue
            FROM silver_gold.contact_360 c
            WHERE c.total_revenue > 0
            ORDER BY c.total_revenue DESC
            LIMIT 10
        """)
        st.dataframe(top_spenders, use_container_width=True)

    # Orders by month
    st.subheader("Orders Over Time")
    monthly = load_data("""
        SELECT FORMAT_DATE('%Y-%m', DATE(CAST(CAST(order_date_key AS STRING) AS DATE FORMAT 'YYYYMMDD'))) AS month,
               COUNT(*) AS orders,
               ROUND(SUM(net_revenue), 0) AS revenue
        FROM silver_gold.fact_orders
        WHERE order_date_key > 20220101
        GROUP BY month ORDER BY month
    """)
    if not monthly.empty:
        st.line_chart(monthly.set_index("month")[["orders"]])


# ═══════════════════════════════════════════
#  EVENT INTELLIGENCE
# ═══════════════════════════════════════════
elif page == "Event Intelligence":
    st.title("Event Intelligence")

    # Summary metrics
    event_stats = load_data("""
        SELECT
            COUNT(*) AS total_registrations,
            COUNT(DISTINCT event_name) AS unique_events,
            COUNTIF(registration_status = 'registered') AS active_registrations,
            COUNTIF(cancelled_flag = 1) AS cancellations,
            ROUND(SAFE_DIVIDE(COUNTIF(cancelled_flag = 1), COUNT(*)) * 100, 1) AS cancel_rate
        FROM silver_gold.fact_event_attendance
    """)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Registrations", f"{event_stats['total_registrations'].iloc[0]:,}")
    c2.metric("Unique Events/Products", f"{event_stats['unique_events'].iloc[0]:,}")
    c3.metric("Active", f"{event_stats['active_registrations'].iloc[0]:,}")
    c4.metric("Cancelled", f"{event_stats['cancellations'].iloc[0]:,}")
    c5.metric("Cancel Rate", f"{event_stats['cancel_rate'].iloc[0]}%")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top Events/Products by Registrations")
        top_events = load_data("""
            SELECT event_name,
                   COUNT(*) AS registrations,
                   COUNTIF(registration_status = 'registered') AS active,
                   COUNTIF(cancelled_flag = 1) AS cancelled
            FROM silver_gold.fact_event_attendance
            GROUP BY event_name
            ORDER BY registrations DESC
            LIMIT 20
        """)
        st.dataframe(top_events, use_container_width=True)

    with col_right:
        st.subheader("Registrations by Status")
        by_status = load_data("""
            SELECT registration_status, COUNT(*) AS count
            FROM silver_gold.fact_event_attendance
            GROUP BY registration_status
        """)
        st.bar_chart(by_status.set_index("registration_status"))

    st.divider()

    # Event attendees with highest engagement
    st.subheader("Top Event Attendees (by total events)")
    top_attendees = load_data("""
        SELECT c.email, c.full_name, c.company_name,
               c.total_events_registered, c.total_events_active, c.total_events_cancelled,
               c.total_orders, ROUND(c.total_revenue, 0) AS total_revenue,
               c.engagement_score
        FROM silver_gold.contact_360 c
        WHERE c.total_events_registered > 0
        ORDER BY c.total_events_registered DESC
        LIMIT 20
    """)
    st.dataframe(top_attendees, use_container_width=True)

    # Revenue from event attendees vs non-attendees
    st.divider()
    st.subheader("Event Attendees vs Non-Attendees — Revenue Impact")
    revenue_comparison = load_data("""
        SELECT
            CASE WHEN total_events_registered > 0 THEN 'Event Attendee' ELSE 'Non-Attendee' END AS segment,
            COUNT(*) AS contacts,
            ROUND(AVG(total_revenue), 0) AS avg_revenue,
            ROUND(SUM(total_revenue), 0) AS total_revenue,
            ROUND(AVG(engagement_score), 1) AS avg_engagement
        FROM silver_gold.contact_360
        GROUP BY segment
    """)
    st.dataframe(revenue_comparison, use_container_width=True)


# ═══════════════════════════════════════════
#  MARKETING & ENGAGEMENT
# ═══════════════════════════════════════════
elif page == "Marketing & Engagement":
    st.title("Marketing & Engagement")

    # Email metrics
    email_stats = load_data("""
        SELECT
            SUM(sent) AS total_sent,
            SUM(delivered) AS total_delivered,
            SUM(opened) AS total_opened,
            SUM(clicked) AS total_clicked,
            SUM(bounced) AS total_bounced,
            SUM(unsubscribed) AS total_unsubscribed,
            ROUND(SAFE_DIVIDE(SUM(opened), SUM(delivered)) * 100, 1) AS open_rate,
            ROUND(SAFE_DIVIDE(SUM(clicked), SUM(delivered)) * 100, 1) AS click_rate,
            ROUND(SAFE_DIVIDE(SUM(bounced), SUM(sent)) * 100, 1) AS bounce_rate
        FROM silver_gold.fact_marketing_touchpoints
    """)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Emails Sent", f"{email_stats['total_sent'].iloc[0]:,}")
    c2.metric("Delivered", f"{email_stats['total_delivered'].iloc[0]:,}")
    c3.metric("Opened", f"{email_stats['total_opened'].iloc[0]:,}")
    c4.metric("Open Rate", f"{email_stats['open_rate'].iloc[0]}%")
    c5.metric("Click Rate", f"{email_stats['click_rate'].iloc[0]}%")
    c6.metric("Bounce Rate", f"{email_stats['bounce_rate'].iloc[0]}%")

    st.divider()

    # Engagement distribution
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Engagement Score Distribution")
        engagement = load_data("""
            SELECT
                CASE
                    WHEN engagement_score = 0 THEN '0 (Inactive)'
                    WHEN engagement_score < 10 THEN '1-9 (Low)'
                    WHEN engagement_score < 50 THEN '10-49 (Medium)'
                    WHEN engagement_score < 100 THEN '50-99 (High)'
                    ELSE '100+ (Super Engaged)'
                END AS engagement_tier,
                COUNT(*) AS contacts
            FROM silver_gold.contact_360
            GROUP BY engagement_tier
            ORDER BY MIN(engagement_score)
        """)
        st.bar_chart(engagement.set_index("engagement_tier"))

    with col_right:
        st.subheader("Top Engaged Contacts")
        top_engaged = load_data("""
            SELECT email, full_name, company_name,
                   engagement_score, total_orders, total_form_submissions,
                   total_emails_opened, total_events_registered
            FROM silver_gold.contact_360
            ORDER BY engagement_score DESC
            LIMIT 10
        """)
        st.dataframe(top_engaged, use_container_width=True)

    # Form submissions
    st.divider()
    st.subheader("Form Submissions by Source")
    forms = load_data("""
        SELECT source_system, COUNT(*) AS submissions
        FROM silver_gold.fact_form_submissions
        GROUP BY source_system
    """)
    st.bar_chart(forms.set_index("source_system"))


# ═══════════════════════════════════════════
#  CONTACT LOOKUP
# ═══════════════════════════════════════════
elif page == "Contact Lookup":
    st.title("Contact Lookup — 360 View")

    search = st.text_input("Search by email, name, or company")

    if search and len(search) >= 3:
        results = load_data(f"""
            SELECT *
            FROM silver_gold.contact_360
            WHERE LOWER(email) LIKE '%{search.lower()}%'
               OR LOWER(full_name) LIKE '%{search.lower()}%'
               OR LOWER(company_name) LIKE '%{search.lower()}%'
            ORDER BY engagement_score DESC
            LIMIT 50
        """)

        if results.empty:
            st.warning("No contacts found")
        else:
            st.write(f"Found {len(results)} contacts")

            for _, person in results.head(10).iterrows():
                with st.expander(f"{person['full_name'] or person['email']} — {person['company_name'] or 'No company'} (Score: {person['engagement_score']})"):
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.markdown("**Identity**")
                        st.write(f"Email: {person['email']}")
                        st.write(f"Phone: {person['phone'] or 'N/A'}")
                        st.write(f"City: {person['city'] or 'N/A'}")
                        st.write(f"LinkedIn: {person['linkedin_url'] or 'N/A'}")
                        st.write(f"Sources: {person['found_in_systems']}")

                    with col2:
                        st.markdown("**Engagement**")
                        st.write(f"Orders: {person['total_orders']} (₹{person['total_revenue']:,.0f})")
                        st.write(f"Forms: {person['total_form_submissions']}")
                        st.write(f"Events: {person['total_events_registered']}")
                        st.write(f"Emails Opened: {person['total_emails_opened']}")
                        st.write(f"Emails Clicked: {person['total_emails_clicked']}")

                    with col3:
                        st.markdown("**Properties**")
                        st.write(f"Properties: {person['total_properties_interacted']}")
                        st.write(f"Paid: {person['total_paid_properties']}")
                        if person['properties_interacted_names']:
                            st.write(f"Names: {person['properties_interacted_names']}")
                        if person['all_emails']:
                            st.write(f"All emails: {person['all_emails']}")
    else:
        st.info("Type at least 3 characters to search")


# ── Footer ──
st.sidebar.divider()
st.sidebar.caption("Inc42 Data Warehouse v1.0")
st.sidebar.caption("Data refreshed daily at midnight IST")
