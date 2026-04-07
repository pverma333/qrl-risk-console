import logging
from fastapi import FastAPI
from app.routers import chain, vix, scenario, portfolio, var

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Quant Risk Console API",
    description="EOD derivatives risk console for Indian Index F&O",
    version="0.1.0",
)

app.include_router(chain.router)
app.include_router(vix.router)
app.include_router(scenario.router)
app.include_router(portfolio.router)
app.include_router(var.router)

@app.get("/health")
def health():
    return {"status": "ok"}

#run
"""
lsof -i :8000
kill -9 *pids*

uvicorn app.main:app --reload
"""

#launch dashboard
"""
cd /Users/priyamverma/Documents/GitHub/qrl-risk-console
streamlit run dashboard/app.py
"""
