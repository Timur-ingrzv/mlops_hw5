import streamlit as st

st.set_page_config(
    page_title="Antifraud service",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

# Make pages
fraud_page = st.Page(
    "pages/upload_file.py",
    title="Scoring",
    icon=":material/dashboard:",
    default=True,
)

monitor_page = st.Page(
    "pages/monitor.py",
    title="Monitoring",
    icon=":material/analytics:",
)

pg = st.navigation(
    [fraud_page, monitor_page],
    position="hidden",
)

with st.sidebar:
    st.title("Fraud Detector", icon=":material/shield:")
    st.caption("ML Fraud Service")

    st.divider()

    st.markdown("### Navigation")

    st.page_link(
        fraud_page,
        label="Score transactions",
        icon=":material/dashboard:",
    )

    st.page_link(
        monitor_page,
        label="Show results",
        icon=":material/analytics:",
    )

pg.run()