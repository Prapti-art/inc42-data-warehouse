"""
Inc42 Data Warehouse — Executive Dashboard
Secured with Google OAuth (@inc42.com only)

Run locally: streamlit run dashboard/app.py
Deploy: see dashboard/deploy.sh
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
    """Simple password gate. Replace with Google OAuth for production."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.markdown("""
    <div style="display: flex; justify-content: center; align-items: center; height: 60vh;">
        <div style="text-align: center; max-width: 400px;">
            <h1>Inc42 Data Warehouse</h1>
            <p style="color: #888;">Enter credentials to continue</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        password = st.text_input("Password", type="password", key="pwd_input")
        if st.button("Login", use_container_width=True):
            # Set this password in Streamlit secrets or environment
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
#  DATA LOADING (cached 5 minutes)
# ══════════════════════════════════════════════
@st.cache_data(ttl=300)
def query(sql):
    bq = bigquery.Client(project=BQ_PROJECT)
    return bq.query(sql).to_dataframe()

@st.cache_data(ttl=300)
def load_contact_360():
    return query("SELECT * FROM silver_gold.contact_360")

@st.cache_data(ttl=300)
def load_company_360():
    return query("SELECT * FROM silver_gold.company_360")

@st.cache_data(ttl=300)
def load_fact_orders():
    return query("SELECT * FROM silver_gold.fact_orders")

@st.cache_data(ttl=300)
def load_fact_forms():
    return query("SELECT * FROM silver_gold.fact_form_submissions")

@st.cache_data(ttl=300)
def load_fact_marketing():
    return query("SELECT * FROM silver_gold.fact_marketing_touchpoints")


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

    df = load_contact_360()

    # Row 1: Key metrics
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Contacts", f"{len(df):,}")
    c2.metric("Paying Customers", f"{(df['total_orders'] > 0).sum():,}")
    c3.metric("Newsletter Subs", f"{(df['total_newsletters_subscribed'] > 0).sum():,}")
    c4.metric("Total Revenue", f"₹{df['total_revenue'].sum():,.0f}")
    c5.metric("Avg Engagement", f"{df['engagement_score'].mean():.1f}")
    c6.metric("Email Reachable", f"{(df['email_reachability'] == 'reachable').sum():,}")

    st.divider()

    # Row 2: Company intelligence
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Unicorn Contacts", f"{df['company_is_unicorn'].sum():,}")
    c2.metric("Soonicorn Contacts", f"{df['company_is_soonicorn'].sum():,}")
    c3.metric("FAST42 Contacts", f"{df['company_is_fast42_winner'].sum():,}")
    c4.metric("Premier Alumni", f"{df['is_premier_institute_alumni'].sum():,}")
    c5.metric("Investors", f"{df['is_investor'].sum():,}")

    st.divider()

    # Row 3: Charts
    col_left, col_mid, col_right = st.columns(3)

    with col_left:
        st.subheader("Contacts by Source Count")
        source_dist = df['source_count'].value_counts().sort_index().head(10)
        st.bar_chart(source_dist)

    with col_mid:
        st.subheader("Email Reachability")
        reach = df['email_reachability'].value_counts()
        st.bar_chart(reach)

    with col_right:
        st.subheader("Top 15 Cities")
        cities = df[df['city'].notna() & (df['city'] != '')]['city'].value_counts().head(15)
        st.bar_chart(cities)

    # Row 4: Engagement distribution
    st.subheader("Engagement Score Distribution")
    bins = [0, 1, 10, 50, 100, 500, float('inf')]
    labels = ['0 (Inactive)', '1-9 (Low)', '10-49 (Medium)', '50-99 (High)', '100-499 (Very High)', '500+ (Super)']
    df['engagement_tier'] = pd.cut(df['engagement_score'], bins=bins, labels=labels, right=False)
    engagement = df['engagement_tier'].value_counts().reindex(labels)
    st.bar_chart(engagement)

    # Row 5: Company sectors
    st.subheader("Top Company Sectors (from Inc42 Thesis)")
    sectors = df[df['company_sector'].notna()]['company_sector'].str.split(', ').explode().value_counts().head(15)
    st.bar_chart(sectors)


# ══════════════════════════════════════════════
#  PAGE 2: REVENUE & PRODUCTS
# ══════════════════════════════════════════════
elif page == "Revenue & Products":
    st.title("Revenue & Products")

    orders = load_fact_orders()

    # Key metrics
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Orders", f"{len(orders):,}")
    c2.metric("Completed", f"{(orders['is_completed'] == 1).sum():,}")
    c3.metric("Revenue", f"₹{orders[orders['is_completed'] == 1]['net_revenue'].sum():,.0f}")
    c4.metric("Refunded", f"{(orders['is_refunded'] == 1).sum():,}")
    c5.metric("Cancelled", f"{(orders['is_cancelled'] == 1).sum():,}")
    avg_order = orders[orders['is_completed'] == 1]['net_revenue'].mean()
    c6.metric("Avg Order Value", f"₹{avg_order:,.0f}" if pd.notna(avg_order) else "N/A")

    st.divider()

    # Revenue by product
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Revenue by Product")
        product_rev = orders[orders['is_completed'] == 1].groupby('product_name')['net_revenue'].sum().sort_values(ascending=False)
        st.bar_chart(product_rev)

    with col_right:
        st.subheader("Orders by Membership Duration")
        duration = orders[orders['membership_duration'].notna()]['membership_duration'].value_counts()
        st.bar_chart(duration)

    # Orders by status
    st.subheader("Orders by Status")
    status = orders['order_status'].value_counts()
    st.bar_chart(status)

    # Top spenders
    st.subheader("Top 20 Spenders")
    df = load_contact_360()
    top_spenders = df[df['total_revenue'] > 0].nlargest(20, 'total_revenue')[
        ['email', 'first_name', 'last_name', 'company_name', 'total_orders', 'total_revenue', 'total_membership_orders', 'company_sector']
    ].reset_index(drop=True)
    st.dataframe(top_spenders, use_container_width=True)

    # Product breakdown table
    st.subheader("Product Performance Detail")
    product_detail = orders.groupby(['product_name', 'product_type', 'membership_duration']).agg(
        orders=('order_id', 'count'),
        completed=('is_completed', 'sum'),
        revenue=('net_revenue', lambda x: x[orders.loc[x.index, 'is_completed'] == 1].sum()),
        refunded=('is_refunded', 'sum'),
        cancelled=('is_cancelled', 'sum'),
    ).reset_index().sort_values('orders', ascending=False)
    st.dataframe(product_detail, use_container_width=True)


# ══════════════════════════════════════════════
#  PAGE 3: NEWSLETTER & MARKETING
# ══════════════════════════════════════════════
elif page == "Newsletter & Marketing":
    st.title("Newsletter & Marketing")

    df = load_contact_360()

    # Newsletter subscriber counts
    st.subheader("Newsletter Subscribers")
    newsletters = {
        'Daily': (df['daily_newsletter'] == 'subscribed').sum(),
        'Weekly': (df['weekly_newsletter'] == 'subscribed').sum(),
        'InDepth': (df['indepth_newsletter'] == 'subscribed').sum(),
        'Moneyball': (df['moneyball_newsletter'] == 'subscribed').sum(),
        'TheOutline': (df['theoutline_newsletter'] == 'subscribed').sum(),
        'Markets': (df['markets_newsletter'] == 'subscribed').sum(),
    }

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Daily", f"{newsletters['Daily']:,}")
    c2.metric("Weekly", f"{newsletters['Weekly']:,}")
    c3.metric("InDepth", f"{newsletters['InDepth']:,}")
    c4.metric("Moneyball", f"{newsletters['Moneyball']:,}")
    c5.metric("TheOutline", f"{newsletters['TheOutline']:,}")
    c6.metric("Markets", f"{newsletters['Markets']:,}")

    st.bar_chart(pd.Series(newsletters))

    st.divider()

    # Newsletters subscribed distribution
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Newsletters per Person")
        nl_dist = df['total_newsletters_subscribed'].value_counts().sort_index()
        st.bar_chart(nl_dist)

    with col_right:
        st.subheader("WhatsApp & Reachability")
        c1, c2, c3 = st.columns(3)
        c1.metric("WhatsApp Opted In", f"{(df['whatsapp_optin'] == 'Yes').sum():,}")
        c2.metric("Globally Unsubscribed", f"{df['is_globally_unsubscribed'].sum():,}")
        c3.metric("Suppressed", f"{df['is_suppressed'].sum():,}")

    # Email marketing metrics
    st.divider()
    st.subheader("Email Marketing Performance")
    mkt = load_fact_marketing()

    c1, c2, c3, c4, c5 = st.columns(5)
    total_sent = mkt['sent'].sum()
    total_delivered = mkt['delivered'].sum()
    total_opened = mkt['opened'].sum()
    total_clicked = mkt['clicked'].sum()
    total_bounced = mkt['bounced'].sum()

    c1.metric("Sent", f"{total_sent:,}")
    c2.metric("Delivered", f"{total_delivered:,}")
    c3.metric("Open Rate", f"{(total_opened/total_delivered*100) if total_delivered > 0 else 0:.1f}%")
    c4.metric("Click Rate", f"{(total_clicked/total_delivered*100) if total_delivered > 0 else 0:.1f}%")
    c5.metric("Bounce Rate", f"{(total_bounced/total_sent*100) if total_sent > 0 else 0:.1f}%")

    # Newsletter subscribers who also pay
    st.divider()
    st.subheader("Newsletter Subscribers who are Paying Customers")
    for nl_name, nl_col in [('Daily', 'daily_newsletter'), ('Weekly', 'weekly_newsletter'),
                            ('Moneyball', 'moneyball_newsletter'), ('InDepth', 'indepth_newsletter'),
                            ('TheOutline', 'theoutline_newsletter')]:
        subs = df[df[nl_col] == 'subscribed']
        paying = subs[subs['total_orders'] > 0]
        pct = (len(paying) / len(subs) * 100) if len(subs) > 0 else 0
        st.write(f"**{nl_name}**: {len(subs):,} subscribers → {len(paying):,} paying ({pct:.1f}%)")


# ══════════════════════════════════════════════
#  PAGE 4: EVENTS & PROGRAMS
# ══════════════════════════════════════════════
elif page == "Events & Programs":
    st.title("Events & Programs")

    forms = load_fact_forms()

    # Category overview
    st.subheader("Submissions by Category")
    cat_counts = forms['form_category'].value_counts()

    c1, c2, c3, c4, c5 = st.columns(5)
    for i, (cat, count) in enumerate(cat_counts.head(5).items()):
        [c1, c2, c3, c4, c5][i].metric(cat, f"{count:,}")

    st.bar_chart(cat_counts)

    st.divider()

    # FAST42
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("FAST42 Applications by Edition")
        fast42 = forms[forms['form_category'] == 'FAST42'].groupby('edition').size().sort_values(ascending=False)
        if not fast42.empty:
            st.bar_chart(fast42)
        else:
            st.info("No FAST42 data")

    with col_right:
        st.subheader("D2C Events by Type")
        d2c = forms[forms['form_category'] == 'D2C Event'].groupby(['form_subcategory', 'edition']).size().reset_index(name='count')
        if not d2c.empty:
            st.dataframe(d2c.sort_values('count', ascending=False), use_container_width=True)
        else:
            st.info("No D2C event data")

    st.divider()

    # AI Workshop
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("AI Workshop by Audience")
        ai = forms[forms['form_category'] == 'AI Workshop'].groupby('form_subcategory').size().sort_values(ascending=False)
        if not ai.empty:
            st.bar_chart(ai)

    with col_right:
        st.subheader("GenAI Summit")
        genai = forms[forms['form_category'] == 'GenAI Summit'].groupby(['form_subcategory', 'edition']).size().reset_index(name='count')
        if not genai.empty:
            st.dataframe(genai, use_container_width=True)

    # Report downloads
    st.divider()
    st.subheader("Report Downloads by Type")
    reports = forms[forms['form_category'] == 'Report Download'].groupby(['form_subcategory', 'edition']).size().reset_index(name='downloads')
    if not reports.empty:
        st.dataframe(reports.sort_values('downloads', ascending=False), use_container_width=True)

    # Startup programs
    st.subheader("Startup Programs")
    startup = forms[forms['form_category'] == 'Startup Program'].groupby('form_subcategory').size().sort_values(ascending=False)
    if not startup.empty:
        st.bar_chart(startup)

    # Full form breakdown
    st.divider()
    st.subheader("Full Form Breakdown")
    form_detail = forms.groupby(['form_category', 'form_subcategory', 'edition', 'source_system']).size().reset_index(name='submissions')
    st.dataframe(form_detail.sort_values('submissions', ascending=False), use_container_width=True, height=400)

    csv = form_detail.to_csv(index=False)
    st.download_button("Download Form Data", csv, "form_submissions.csv", "text/csv")


# ══════════════════════════════════════════════
#  PAGE 5: LEAD DISCOVERY
# ══════════════════════════════════════════════
elif page == "Lead Discovery":
    st.title("Lead Discovery")
    st.caption("Find and export contact lists based on filters")

    df = load_contact_360()

    # Filters
    st.subheader("Filters")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        sector_options = sorted(df[df['company_sector'].notna()]['company_sector'].str.split(', ').explode().unique())
        sector_filter = st.multiselect("Company Sector", sector_options)

    with col2:
        city_options = sorted(df[df['city'].notna() & (df['city'] != '')]['city'].unique())
        city_filter = st.multiselect("City", city_options[:50])

    with col3:
        seniority_options = sorted(df[df['seniority'].notna() & (df['seniority'] != '')]['seniority'].unique())
        seniority_filter = st.multiselect("Seniority", seniority_options)

    with col4:
        min_engagement = st.slider("Min Engagement Score", 0, 500, 0)

    col5, col6, col7, col8 = st.columns(4)

    with col5:
        has_orders = st.checkbox("Has orders")
    with col6:
        has_email = st.checkbox("Email reachable only")
    with col7:
        unicorn_only = st.checkbox("Unicorn company only")
    with col8:
        premier_alumni = st.checkbox("Premier institute alumni")

    col9, col10, col11, col12 = st.columns(4)

    with col9:
        min_sources = st.slider("Min source count", 1, 6, 1)
    with col10:
        newsletter_filter = st.multiselect("Newsletter subscribed", ['Daily', 'Weekly', 'InDepth', 'Moneyball', 'TheOutline', 'Markets'])
    with col11:
        fast42_filter = st.checkbox("FAST42 winner company")
    with col12:
        has_linkedin = st.checkbox("Has LinkedIn URL")

    # Apply filters
    filtered = df.copy()
    filtered = filtered[filtered['engagement_score'] >= min_engagement]
    filtered = filtered[filtered['source_count'] >= min_sources]

    if sector_filter:
        filtered = filtered[filtered['company_sector'].fillna('').apply(
            lambda x: any(s in x for s in sector_filter)
        )]
    if city_filter:
        filtered = filtered[filtered['city'].isin(city_filter)]
    if seniority_filter:
        filtered = filtered[filtered['seniority'].isin(seniority_filter)]
    if has_orders:
        filtered = filtered[filtered['total_orders'] > 0]
    if has_email:
        filtered = filtered[filtered['email_reachability'] == 'reachable']
    if unicorn_only:
        filtered = filtered[filtered['company_is_unicorn'] == True]
    if premier_alumni:
        filtered = filtered[filtered['is_premier_institute_alumni'] == True]
    if fast42_filter:
        filtered = filtered[filtered['company_is_fast42_winner'] == True]
    if has_linkedin:
        filtered = filtered[filtered['linkedin_url'].notna() & (filtered['linkedin_url'] != '')]

    nl_map = {
        'Daily': 'daily_newsletter', 'Weekly': 'weekly_newsletter',
        'InDepth': 'indepth_newsletter', 'Moneyball': 'moneyball_newsletter',
        'TheOutline': 'theoutline_newsletter', 'Markets': 'markets_newsletter'
    }
    for nl in newsletter_filter:
        filtered = filtered[filtered[nl_map[nl]] == 'subscribed']

    # Results
    st.divider()
    st.subheader(f"Results: {len(filtered):,} contacts")

    # Summary of filtered set
    if len(filtered) > 0:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Contacts", f"{len(filtered):,}")
        c2.metric("With Email", f"{filtered['email'].notna().sum():,}")
        c3.metric("With Phone", f"{filtered['phone'].notna().sum():,}")
        c4.metric("Avg Engagement", f"{filtered['engagement_score'].mean():.1f}")
        c5.metric("Total Revenue", f"₹{filtered['total_revenue'].sum():,.0f}")

    # Display columns
    display_cols = [
        'email', 'first_name', 'last_name', 'company_name', 'designation', 'seniority',
        'city', 'phone', 'linkedin_url', 'company_sector',
        'engagement_score', 'total_orders', 'total_revenue',
        'total_newsletters_subscribed', 'total_form_submissions',
        'company_total_funding_usd', 'company_is_unicorn',
        'source_count', 'found_in_systems', 'email_reachability'
    ]
    available_cols = [c for c in display_cols if c in filtered.columns]

    st.dataframe(
        filtered[available_cols].sort_values('engagement_score', ascending=False).head(1000),
        use_container_width=True,
        height=500
    )

    # Download
    export_cols = [
        'email', 'first_name', 'last_name', 'phone', 'company_name', 'designation',
        'seniority', 'city', 'state', 'country', 'linkedin_url',
        'company_sector', 'company_sub_sectors', 'company_business_model',
        'company_total_funding_usd', 'company_last_funding_stage',
        'company_employee_count', 'company_investors',
        'company_is_unicorn', 'company_is_soonicorn', 'company_is_fast42_winner',
        'company_inc42_tags',
        'dl_education_institution', 'dl_education_degree', 'is_premier_institute_alumni',
        'engagement_score', 'total_orders', 'total_revenue', 'net_ltv',
        'total_form_submissions', 'total_events_registered',
        'total_emails_opened', 'total_emails_clicked',
        'daily_newsletter', 'weekly_newsletter', 'moneyball_newsletter',
        'total_newsletters_subscribed', 'whatsapp_optin', 'email_reachability',
        'source_count', 'found_in_systems', 'all_emails', 'all_phones'
    ]
    export_available = [c for c in export_cols if c in filtered.columns]
    csv = filtered[export_available].to_csv(index=False)
    st.download_button(
        f"Download {len(filtered):,} contacts as CSV",
        csv, "inc42_leads.csv", "text/csv",
        use_container_width=True
    )


# ══════════════════════════════════════════════
#  PAGE 6: CONTACT LOOKUP
# ══════════════════════════════════════════════
elif page == "Contact Lookup":
    st.title("Contact Lookup — Full 360 View")

    search = st.text_input("Search by email, name, or company (min 3 characters)")

    if search and len(search) >= 3:
        df = load_contact_360()
        search_lower = search.lower()

        results = df[
            df['email'].fillna('').str.lower().str.contains(search_lower, na=False) |
            df['first_name'].fillna('').str.lower().str.contains(search_lower, na=False) |
            df['last_name'].fillna('').str.lower().str.contains(search_lower, na=False) |
            df['company_name'].fillna('').str.lower().str.contains(search_lower, na=False)
        ].sort_values('engagement_score', ascending=False).head(50)

        if results.empty:
            st.warning("No contacts found")
        else:
            st.write(f"Found **{len(results)}** contacts")

            for _, p in results.head(20).iterrows():
                name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip() or p['email']
                company = p.get('company_name', '') or 'No company'
                score = p.get('engagement_score', 0)

                with st.expander(f"{name} — {company} (Score: {score:.0f})"):
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.markdown("**Identity**")
                        st.write(f"Email: {p['email']}")
                        st.write(f"Phone: {p.get('phone') or 'N/A'}")
                        st.write(f"City: {p.get('city') or 'N/A'}")
                        if p.get('linkedin_url'):
                            st.write(f"LinkedIn: {p['linkedin_url']}")
                        st.write(f"Sources ({p['source_count']}): {p.get('found_in_systems', '')}")
                        if p.get('all_emails'):
                            st.write(f"All emails: {p['all_emails']}")

                    with col2:
                        st.markdown("**Professional**")
                        st.write(f"Designation: {p.get('designation') or 'N/A'}")
                        st.write(f"Seniority: {p.get('seniority') or 'N/A'}")
                        if p.get('dl_current_company'):
                            st.write(f"DL Company: {p['dl_current_company']}")
                        if p.get('dl_education_institution'):
                            st.write(f"Education: {p['dl_education_institution']}")
                            if p.get('dl_education_degree'):
                                st.write(f"Degree: {p['dl_education_degree']}")
                        if p.get('is_premier_institute_alumni'):
                            badges = []
                            if p.get('is_iit_alumni'): badges.append("IIT")
                            if p.get('is_iim_alumni'): badges.append("IIM")
                            if p.get('is_isb_alumni'): badges.append("ISB")
                            if p.get('is_bits_alumni'): badges.append("BITS")
                            if p.get('is_global_top_alumni'): badges.append("Global Top")
                            st.write(f"Alumni: {', '.join(badges)}")
                        if p.get('is_investor'):
                            st.write(f"Investor: {p.get('person_investments_made', 0)} investments")
                            if p.get('person_portfolio_companies'):
                                st.write(f"Portfolio: {p['person_portfolio_companies'][:100]}")

                    with col3:
                        st.markdown("**Company Intel**")
                        st.write(f"Sector: {p.get('company_sector') or 'N/A'}")
                        if p.get('company_total_funding_usd') and p['company_total_funding_usd'] > 0:
                            st.write(f"Funding: ${p['company_total_funding_usd']:,.0f}")
                            st.write(f"Stage: {p.get('company_last_funding_stage') or 'N/A'}")
                        if p.get('company_employee_count'):
                            st.write(f"Employees: {p['company_employee_count']:,.0f}")
                        if p.get('company_investors'):
                            st.write(f"Investors: {str(p['company_investors'])[:120]}")
                        if p.get('company_inc42_tags'):
                            st.write(f"Inc42 Tags: {p['company_inc42_tags']}")
                        flags = []
                        if p.get('company_is_unicorn'): flags.append("Unicorn")
                        if p.get('company_is_soonicorn'): flags.append("Soonicorn")
                        if p.get('company_is_fast42_winner'): flags.append("FAST42")
                        if flags:
                            st.write(f"Status: {', '.join(flags)}")

                    with col4:
                        st.markdown("**Engagement**")
                        st.write(f"Orders: {p['total_orders']} (₹{p['total_revenue']:,.0f})")
                        st.write(f"Forms: {p['total_form_submissions']}")
                        st.write(f"Events: {p['total_events_registered']}")
                        st.write(f"Emails Opened: {p['total_emails_opened']}")
                        st.write(f"Emails Clicked: {p['total_emails_clicked']}")
                        st.write(f"Newsletters: {p['total_newsletters_subscribed']}")
                        st.write(f"Reachability: {p.get('email_reachability', 'N/A')}")
                        if p.get('properties_interacted_names'):
                            st.write(f"Properties: {p['properties_interacted_names'][:150]}")

    elif search:
        st.info("Type at least 3 characters")
    else:
        st.info("Search for any contact by email, name, or company")


# ══════════════════════════════════════════════
#  PAGE 7: COMPANY INTELLIGENCE
# ══════════════════════════════════════════════
elif page == "Company Intelligence":
    st.title("Company Intelligence")

    companies = load_company_360()

    # Key metrics
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Companies", f"{len(companies):,}")
    c2.metric("With Funding", f"{(companies['total_funding_usd'] > 0).sum():,}")
    c3.metric("Profitable", f"{companies['is_profitable'].sum():,}")
    c4.metric("Unicorns", f"{companies['is_unicorn'].sum():,}")
    c5.metric("Soonicorns", f"{companies['is_soonicorn'].sum():,}")
    c6.metric("FAST42 Winners", f"{companies['is_fast42_winner'].sum():,}")

    st.divider()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        sector_options = sorted(companies[companies['sector'].notna()]['sector'].str.split(', ').explode().unique())
        sector_filter = st.multiselect("Filter by Sector", sector_options, key="company_sector_filter")
    with col2:
        min_funding = st.number_input("Min Funding (USD)", value=0, step=1000000)
    with col3:
        tag_filter = st.multiselect("Inc42 Tags", ['Unicorn', 'Soonicorn', 'Minicorn', 'FAST42', '30 Startups To Watch'])

    filtered = companies.copy()
    if sector_filter:
        filtered = filtered[filtered['sector'].fillna('').apply(lambda x: any(s in x for s in sector_filter))]
    if min_funding > 0:
        filtered = filtered[filtered['total_funding_usd'] >= min_funding]
    for tag in tag_filter:
        tag_col = f"is_{tag.lower().replace(' ', '_').replace('30_startups_to_watch', '30_startups_to_watch')}"
        if tag == 'Unicorn': filtered = filtered[filtered['is_unicorn'] == True]
        elif tag == 'Soonicorn': filtered = filtered[filtered['is_soonicorn'] == True]
        elif tag == 'Minicorn': filtered = filtered[filtered['is_minicorn'] == True]
        elif tag == 'FAST42': filtered = filtered[filtered['is_fast42_winner'] == True]
        elif tag == '30 Startups To Watch': filtered = filtered[filtered['is_30_startups_to_watch'] == True]

    st.subheader(f"Companies: {len(filtered):,}")

    display_cols = [
        'company_name', 'sector', 'sub_sectors', 'city', 'founded_year',
        'total_funding_usd', 'last_funding_stage', 'investor_count',
        'employee_count', 'is_profitable', 'monthly_visits',
        'inc42_tags', 'contacts_in_warehouse', 'total_revenue_from_company'
    ]
    available = [c for c in display_cols if c in filtered.columns]

    st.dataframe(
        filtered[available].sort_values('total_funding_usd', ascending=False).head(500),
        use_container_width=True, height=500
    )

    # Top sectors by funding
    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top Sectors by Funding")
        sector_funding = filtered[filtered['sector'].notna()].groupby(
            filtered['sector'].str.split(', ').explode()
        )['total_funding_usd'].sum().sort_values(ascending=False).head(15)
        st.bar_chart(sector_funding)

    with col_right:
        st.subheader("Companies with Inc42 Contacts")
        with_contacts = filtered[filtered['contacts_in_warehouse'] > 0].nlargest(15, 'contacts_in_warehouse')[
            ['company_name', 'sector', 'contacts_in_warehouse', 'total_revenue_from_company']
        ]
        st.dataframe(with_contacts, use_container_width=True)

    # Download
    csv = filtered.to_csv(index=False)
    st.download_button(f"Download {len(filtered):,} companies as CSV", csv, "companies.csv", "text/csv")


# ── Footer ──
st.sidebar.divider()
st.sidebar.caption("v2.0 — Inc42 Data Warehouse")
st.sidebar.caption(f"330K contacts | 75K companies | 25M+ raw rows")
