import streamlit as st

st.set_page_config(
    page_title="QRL Console",
    page_icon="📊",
    layout="wide",
)

# --- Header Section ---
st.title("QRL Console")
st.markdown("#### Institutional Risk Engine for Indian Index Derivatives")
st.markdown("*EOD Analytics • Black-Scholes Pricing • Historical VaR Simulation*")
st.divider()

# --- Feature Grid ---
col1, col2, col3, col4 = st.columns(4)

# We use a fixed height (e.g., 200 pixels) so the buttons align at the bottom
container_height = 200

with col1:
    with st.container(height=container_height):
        st.subheader("Market Explorer")
        st.write("Analyze NIFTY/BANKNIFTY chains. View settlement prices, IV, and Greeks. Includes IV smile and delta charts.")
    st.page_link("pages/1_Market_Explorer.py", label="Open Market Explorer", use_container_width=True)

with col2:
    with st.container(height=container_height):
        st.subheader("Portfolio Risk")
        st.write("Upload positions for EOD pricing and MTM. Stress-test against spot, vol, and rate shocks.")
    st.page_link("pages/2_Portfolio_Risk.py", label="Open Portfolio Risk", use_container_width=True)

with col3:
    with st.container(height=container_height):
        st.subheader("Scenario Lab")
        st.write("Single-contract stress testing. Apply custom spot, vol, and rate shocks to calculate shocked pricing.")
    st.page_link("pages/3_Scenario_Lab.py", label="Open Scenario Lab", use_container_width=True)

with col4:
    with st.container(height=container_height):
        st.subheader("VaR / CVaR")
        st.write("252-day historical simulation. Compute VaR/CVaR at 95%/99% confidence with PnL distributions.")
    st.page_link("pages/4_VaR_CVaR.py", label="Open VaR / CVaR", use_container_width=True)

# --- Technical Footer ---
st.divider()
st.caption(
    "**Data:** NSE EOD (₹) | **Model:** Black-Scholes (Actual/365) | "
    "**Rates:** Linear Interpolation (3M/6M/1Y G-Bond) | **Dividends:** NSE Index Yield"
)
