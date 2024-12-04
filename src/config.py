from dataclasses import dataclass
from typing import Optional

from src.intergrations.abstract_integrations import InputIntegration, OutputIntegration
from transformation import Transformation


@dataclass
class Config:
    input: InputIntegration
    output: OutputIntegration
    transform: Optional[Transformation] = None

