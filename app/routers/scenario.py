from fastapi import APIRouter

from app.schemas.scenario import ScenarioParameters, ScenarioResponse

router = APIRouter(prefix="/scenario", tags=["scenario"])


@router.post("/", response_model=ScenarioResponse)
def scenario_endpoint(params: ScenarioParameters):
    return ScenarioResponse(parameters_received=params)
