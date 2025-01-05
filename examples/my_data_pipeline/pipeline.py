from pathlib import Path

import yaml
from pandas import DataFrame, read_csv
from pydantic import BaseModel

from pipestruct import PipelineDefinition, Transformation, initialize_pipeline


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Define your transaformations by inheriting the Transformation class.  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
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


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Optionally you may create a pydantic model using PipelineDefinition   #
# so that you have express your pipeline in a nice-looking config file. #
# See definition.yml                                                    #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class MyPipeline(BaseModel):
    pipeline: PipelineDefinition


def main() -> None:
    # Parse the config file using your PipelineDefinition object
    with (Path(__file__).parent / "definition.yml").open("r") as f:
        my_data_pipe_def = MyPipeline.model_validate(yaml.safe_load(f))

    # Initialise your pipeline by providing the PipelineDefinition
    # and providing the transformations you defined
    pipeline = initialize_pipeline(
        my_data_pipe_def.pipeline,
        available_transformations=[FilterNumInterval, FilterByValue],
    )

    # Load your data
    path_data = Path(__file__).parent / "data.csv"
    df = read_csv(path_data)

    # Run the pipeline
    output = pipeline.apply(df)
    print("Processed df:")
    print(output)


if __name__ == "__main__":
    main()
