import logging
from fastapi import FastAPI
from app.routers import chain, vix, scenario

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Quant Risk Console API",
    description="EOD derivatives risk engine for Indian index options.",
    version="0.1.0",
)

app.include_router(chain.router)
app.include_router(vix.router)
app.include_router(scenario.router)

@app.get("/health")
def health():
    return {"status": "ok"}
