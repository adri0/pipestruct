from typing import cast

from pandas import DataFrame, concat

from pipestruct.transformation import Transformation


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
