from typing import Any, Iterable, Type

from pydantic import BaseModel

from pipestruct.pipeline import Pipeline
from pipestruct.transformation import Transformation


class StepDef(BaseModel):
    type: str
    params: dict[str, Any]


class PipelineDefinition(BaseModel):
    name: str
    steps: list[StepDef]


def initialize_pipeline(
    pipe_def: PipelineDefinition,
    available_transformations: Iterable[Type[Transformation]],
) -> Pipeline:
    available_trasnf = {transf.__name__: transf for transf in available_transformations}
    return Pipeline(
        steps=[
            available_trasnf[step_def.type].model_validate(step_def.params)
            for step_def in pipe_def.steps
        ],
    )
