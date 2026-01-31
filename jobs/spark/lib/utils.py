from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Paths:
    raw: str
    curated: str
    metrics: str
    checkpoint: str

def get_paths(project_root: str) -> Paths:
    return Paths(
        raw=os.path.join(project_root, "data", "raw"),
        curated=os.path.join(project_root, "warehouse", "curated"),
        metrics=os.path.join(project_root, "warehouse", "metrics"),
        checkpoint=os.path.join(project_root, "data", "checkpoint"),
    )
