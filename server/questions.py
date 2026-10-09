"""Load the question set from JSON."""
import json
from dataclasses import dataclass
from pathlib import Path

from .scoring import normalize

# Shown on the phone with each question (see docs/protocol.md, `question`):
# gen1 often mishears a bare short word ("eight" -> "A"), less so a phrase.
DEFAULT_HINT = "Answer in a few words, e.g. \"It's the Pacific\""


@dataclass(frozen=True)
class Question:
    id: str
    text: str
    answers: list[str]  # accepted spoken answers, normalised like transcripts are
    hint: str = DEFAULT_HINT
    phrases: tuple[str, ...] = ()  # accepted answers as written, for recognition context


def load_questions(path: Path) -> list[Question]:
    with open(path, encoding="utf-8") as f:
        items = json.load(f)
    questions = [Question(q["id"], q["text"], list(dict.fromkeys(normalize(a) for a in q["answers"])),
                          q.get("hint", DEFAULT_HINT), tuple(q["answers"])) for q in items]
    if not questions:
        raise ValueError(f"no questions in {path}")
    return questions
