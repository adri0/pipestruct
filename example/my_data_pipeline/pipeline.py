from pathlib import Path

import yaml
from pandas import DataFrame, read_csv
from pydantic import BaseModel

from pipestruct import PipelineDefinition, Transformation, initialize_pipeline


class FilterNumInterval(Transformation):
    column: str
    max_value: float | int = float("+Inf")
    min_value: float | int = float("-Inf")

    def transform(self, df: DataFrame) -> DataFrame:
        return df[
            (df[self.column] <= self.max_value) & (df[self.column] >= self.min_value)
        ]


class FilterByValue(Transformation):
    column: str
    value: str | float | int

    def transform(self, df: DataFrame) -> DataFrame:
        return df[df[self.column] == self.value]


class MyPipeline(BaseModel):
    pipeline: PipelineDefinition


def run() -> None:
    with (Path(__file__).parent / "definition.yml").open("r") as f:
        my_data_pipe_def = MyPipeline.model_validate(yaml.safe_load(f))

    pipeline = initialize_pipeline(
        my_data_pipe_def.pipeline,
        available_transformations=[FilterNumInterval, FilterByValue],
    )

    path_data = Path(__file__).parent / "data.csv"

    df = read_csv(path_data)
    output = pipeline.apply(df)

    print("Processed df:")
    print(output)


if __name__ == "__main__":
    run()
