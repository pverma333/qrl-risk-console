import streamlit as st

st.set_page_config(
    page_title="QRL Risk Console",
    page_icon="📊",
    layout="wide",
)

st.title("QRL Risk Console")
st.markdown("**Institutional-grade Indian Index Derivatives Risk Engine**")
st.divider()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.subheader("Market Explorer")
    st.write("Browse option chains, IV smile, and Greeks for any index, date, and expiry.")
    st.page_link("pages/1_Market_Explorer.py", label="Open Market Explorer →")

with col2:
    st.subheader("Portfolio Risk")
    st.write("Upload a portfolio CSV. Get MtM PnL, scenario PnL, and net Greeks instantly.")
    st.page_link("pages/2_Portfolio_Risk.py", label="Open Portfolio Risk →")

with col3:
    st.subheader("Scenario Lab")
    st.write("Stress test a single contract under custom spot, vol, and rate shocks.")
    st.page_link("pages/3_Scenario_Lab.py", label="Open Scenario Lab →")

with col4:
    st.subheader("VaR / CVaR")
    st.write("Historical simulation VaR and CVaR across 252 trading day scenarios.")
    st.page_link("pages/4_VaR_CVaR.py", label="Open VaR / CVaR →")

st.divider()
st.caption("EOD only. Data sourced from NSE. Pricing model: Black-Scholes. Day count: 365. Rate interpolation: linear (3M/6M/1Y).")
