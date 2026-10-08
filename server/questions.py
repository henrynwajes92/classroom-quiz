"""Load the question set from JSON."""
import json
from dataclasses import dataclass
from pathlib import Path

from .scoring import normalize


@dataclass(frozen=True)
class Question:
    id: str
    text: str
    answers: list[str]  # accepted spoken answers, normalised like transcripts are


def load_questions(path: Path) -> list[Question]:
    with open(path, encoding="utf-8") as f:
        items = json.load(f)
    questions = [Question(q["id"], q["text"], [normalize(a) for a in q["answers"]]) for q in items]
    if not questions:
        raise ValueError(f"no questions in {path}")
    return questions
