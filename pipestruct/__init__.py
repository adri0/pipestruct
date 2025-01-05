from abc import ABC, abstractmethod
from typing import Any, Iterable, Type, cast, final

from pandas import DataFrame, concat
from pydantic import BaseModel


class Transformation(ABC, BaseModel, extra="forbid"):
    @final
    def apply(self, df: DataFrame) -> DataFrame:
        df = df.copy(deep=False)
        df = self.transform(df)
        return df

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass


class StepDef(BaseModel):
    type: str
    params: dict[str, Any]


class PipelineDefinition(BaseModel):
    name: str
    steps: list[StepDef]


class Pipeline(Transformation):
    steps: list[Transformation | list[Transformation]]

    def transform(self, df: DataFrame) -> DataFrame:
        for step in self.steps:
            if isinstance(step, Transformation):
                df = step.apply(df)
            elif isinstance(step, list):
                df_orig = df.copy(deep=False)
                df = concat(
                    cast(Transformation, substep).apply(df_orig) for substep in step
                )
            else:
                raise ValueError(
                    "step must be a Transformation or Iterable[Transformation]"
                )
        return df


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
