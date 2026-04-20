"""Compute similarity scores between pipeline run profiles."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class SimilarityResult:
    pipeline_a: str
    pipeline_b: str
    score: float  # 0.0 (no similarity) to 1.0 (identical profile)
    shared_errors: int
    failure_rate_delta: float

    def __str__(self) -> str:
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"score={self.score:.2f}  shared_errors={self.shared_errors}  "
            f"failure_rate_delta={self.failure_rate_delta:.2f}"
        )


def _failure_rate(runs: List[PipelineRun]) -> float:
    if not runs:
        return 0.0
    return sum(1 for r in runs if r.is_failed()) / len(runs)


def _error_tokens(runs: List[PipelineRun]) -> set:
    tokens: set = set()
    for r in runs:
        if r.error:
            for word in r.error.lower().split():
                tokens.add(word)
    return tokens


def compute_similarity(
    runs_a: List[PipelineRun],
    runs_b: List[PipelineRun],
    name_a: str,
    name_b: str,
) -> SimilarityResult:
    rate_a = _failure_rate(runs_a)
    rate_b = _failure_rate(runs_b)
    rate_delta = abs(rate_a - rate_b)

    tokens_a = _error_tokens(runs_a)
    tokens_b = _error_tokens(runs_b)
    shared = tokens_a & tokens_b
    union = tokens_a | tokens_b
    jaccard = len(shared) / len(union) if union else 1.0

    rate_sim = 1.0 - rate_delta
    score = round(0.5 * jaccard + 0.5 * rate_sim, 4)

    return SimilarityResult(
        pipeline_a=name_a,
        pipeline_b=name_b,
        score=score,
        shared_errors=len(shared),
        failure_rate_delta=round(rate_delta, 4),
    )


def find_similar_pipelines(
    runs: List[PipelineRun],
    threshold: float = 0.6,
) -> List[SimilarityResult]:
    """Return all pipeline pairs whose similarity score >= threshold."""
    from pipewatch.filter import unique_pipelines
    from pipewatch.groupby import group_by_pipeline

    grouped = group_by_pipeline(runs)
    pipelines = sorted(grouped.keys())
    results: List[SimilarityResult] = []

    for i, name_a in enumerate(pipelines):
        for name_b in pipelines[i + 1:]:
            result = compute_similarity(
                grouped[name_a], grouped[name_b], name_a, name_b
            )
            if result.score >= threshold:
                results.append(result)

    results.sort(key=lambda r: r.score, reverse=True)
    return results
