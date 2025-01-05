from abc import ABC, abstractmethod
from typing import final

from pandas import DataFrame
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
