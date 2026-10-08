"""Answer scoring. PLACEHOLDER until CQ-7 (filler words, fuzzy match, speed bonus).

For now: lowercase, strip punctuation, exact match against the accepted
answers (normalised the same way at load time), flat points. Enough to drive
the leaderboard end to end.
"""
import re

POINTS_CORRECT = 100


def normalize(text: str) -> str:
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return " ".join(text.split())


def score(question, transcript: str, answer_seconds: float, time_limit: float) -> tuple[bool, int]:
    """Return (correct, points) for a questions.Question.
    answer_seconds: question start -> final result."""
    correct = normalize(transcript) in question.answers
    return correct, POINTS_CORRECT if correct else 0
