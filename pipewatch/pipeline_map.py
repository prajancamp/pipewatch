"""Pipeline dependency map: track and query upstream/downstream relationships."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import json
from pathlib import Path


@dataclass
class PipelineNode:
    name: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"name": self.name, "upstream": self.upstream, "downstream": self.downstream}

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineNode":
        return cls(name=d["name"], upstream=d.get("upstream", []), downstream=d.get("downstream", []))


@dataclass
class PipelineMap:
    nodes: Dict[str, PipelineNode] = field(default_factory=dict)

    def add_edge(self, upstream: str, downstream: str) -> None:
        """Register a dependency: upstream feeds into downstream."""
        if upstream not in self.nodes:
            self.nodes[upstream] = PipelineNode(name=upstream)
        if downstream not in self.nodes:
            self.nodes[downstream] = PipelineNode(name=downstream)
        if downstream not in self.nodes[upstream].downstream:
            self.nodes[upstream].downstream.append(downstream)
        if upstream not in self.nodes[downstream].upstream:
            self.nodes[downstream].upstream.append(upstream)

    def get_upstream(self, name: str) -> List[str]:
        return self.nodes[name].upstream if name in self.nodes else []

    def get_downstream(self, name: str) -> List[str]:
        return self.nodes[name].downstream if name in self.nodes else []

    def all_pipelines(self) -> List[str]:
        return sorted(self.nodes.keys())

    def to_dict(self) -> dict:
        return {n: node.to_dict() for n, node in self.nodes.items()}

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineMap":
        m = cls()
        m.nodes = {n: PipelineNode.from_dict(v) for n, v in d.items()}
        return m


def _map_path(store_path: str) -> Path:
    return Path(store_path).parent / "pipeline_map.json"


def load_map(store_path: str) -> PipelineMap:
    p = _map_path(store_path)
    if not p.exists():
        return PipelineMap()
    return PipelineMap.from_dict(json.loads(p.read_text()))


def save_map(store_path: str, pm: PipelineMap) -> None:
    p = _map_path(store_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(pm.to_dict(), indent=2))
