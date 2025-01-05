from pandas import DataFrame
from pytest import fixture

from pipestruct import Pipeline, Transformation


class AddConstantColumn(Transformation):
    col: str
    constant: int

    def transform(self, df: DataFrame) -> DataFrame:
        df[self.col] = self.constant
        return df


@fixture()
def sample_df() -> DataFrame:
    data = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["A", "B", "C", "D", "E"],
        "col3": [10.1, 20.2, 30.3, 40.4, 50.5],
    }
    return DataFrame(data)


def test_transform_constant_col(sample_df: DataFrame) -> None:
    const10 = AddConstantColumn(col="constant", constant=10)
    df = const10.apply(sample_df)
    assert list(df["constant"]) == [10] * 5


def test_pipeline(sample_df: DataFrame) -> None:
    const10 = AddConstantColumn(col="constant1", constant=10)
    const20 = AddConstantColumn(col="constant2", constant=20)
    pipe = Pipeline(steps=[const10, const20])
    df = pipe.apply(sample_df)
    assert list(df["constant1"]) == [10] * 5
    assert list(df["constant2"]) == [20] * 5
