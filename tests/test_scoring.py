"""Scoring unit tests: normalisation, matching, points (CQ-7)."""
import json

import pytest

from server.config import QUESTIONS_FILE, ROOT
from server.questions import load_questions
from server.scoring import edit_distance, is_correct, normalize, points_for, score

QUESTIONS = {q.id: q for q in load_questions(QUESTIONS_FILE)}


def ok(qid, text):
    return is_correct(QUESTIONS[qid].answers, text)


@pytest.mark.parametrize("text, expected", [
    ("Paris.", "paris"),
    ("The answer's eight.", "eight"),             # contracted, as Transcribe writes it
    ("The answer is the Pacific.", "pacific"),
    ("The answers the piano.", "piano"),          # gen1's spelling of "the answer's"
    ("I think It's eight legs.", "eight legs"),   # odd caps
    ("I think its the Pacific Ocean.", "pacific ocean"),
    ("Eight legs. I think.", "eight legs"),       # trailing "I think." as its own sentence
    ("The Nile River, I think.", "nile river"),
    ("Um. Paris.", "paris"),
    ("Uh, maybe Mars?", "mars"),
    ("It's a giraffe.", "giraffe"),
    ("A piano.", "piano"),
    ("Tokyo, Japan!", "tokyo japan"),
    ("It’s Jupiter", "jupiter"),             # curly apostrophe
    ("That's Paris.", "paris"),
    ("I'd say an elephant.", "elephant"),
    ("Definitely the Nile.", "nile"),
    ("A", ""),                                    # gen1 mishearing "eight": nothing left
    ("It's a", ""),
    ("", ""),
])
def test_normalize(text, expected):
    assert normalize(text) == expected


def test_normalize_is_idempotent_on_answers():
    assert all(normalize(a) == a for q in QUESTIONS.values() for a in q.answers)


@pytest.mark.parametrize("qid, text", [
    ("france-capital", "Paris."),
    ("france-capital", "I think it's Paris."),
    ("france-capital", "pairs"),            # near miss, accepted: one swapped pair of letters
    ("france-capital", "Parris"),
    ("spider-legs", "The answer's eight."),
    ("spider-legs", "8"),
    ("spider-legs", "ate"),                 # listed explicitly; too short to fuzzy-match
    ("largest-ocean", "It's the Pacific Ocean. I think."),
    ("largest-ocean", "The Pacifc."),
    ("romeo-author", "William Shakespear."),
    ("romeo-author", "Shakespere."),
    ("france-capital", "That's Paris."),
    ("france-capital", "I'd say Paris."),
    ("japan-capital", "Definitely Tokyo."),
    ("japan-capital", "Tokio."),
    ("tallest-animal", "Giraffes."),
    ("egypt-river", "The river Nile."),
    ("piano-keys", "I think It's a piano."),
])
def test_accepted(qid, text):
    assert ok(qid, text)


@pytest.mark.parametrize("qid, text", [
    ("france-capital", "London."),
    ("france-capital", "The answer is Rome."),
    ("france-capital", ""),
    ("spider-legs", "A"),                   # gen1 for a bare "eight": not given points
    ("spider-legs", "It's a"),
    ("spider-legs", "Sits."),
    ("spider-legs", "I think It's 10."),
    ("spider-legs", "The answer is six legs."),
    ("piano-keys", "PN."),
    ("piano-keys", "Gino."),
    ("largest-ocean", "Civic."),
    ("largest-ocean", "The Atlantic."),
    ("largest-ocean", "The answer is the Arctic ocean."),
    ("egypt-river", "No."),
    ("japan-capital", "So."),               # "Seoul"
    ("japan-capital", "Lusaka."),
    ("red-planet", "Venus."),
    ("largest-planet", "Saturn."),
    ("romeo-author", "Charles Dickens."),
    # numbers: digits and number words are never near misses
    ("spider-legs", "6 legs"), ("spider-legs", "4 legs"), ("spider-legs", "7 legs"),
    ("spider-legs", "9 legs"), ("spider-legs", "18 legs"), ("spider-legs", "It's 6 legs."),
    ("spider-legs", "eighty"), ("spider-legs", "eighty legs"), ("spider-legs", "eighteen"),
    # a wrong word next to a shared one gets no budget from the shared one
    ("red-planet", "planet earth"), ("red-planet", "The planet Earth."),
    ("egypt-river", "The river Niger."), ("egypt-river", "Niger river"), ("egypt-river", "The Niger River"),
    ("largest-planet", "The planet Saturn."),
    # weaker near misses: first/last letter differs, too many edits, or too short
    ("bees-make", "money"), ("banana-color", "hello"), ("banana-color", "fellow"),
    ("banana-color", "mellow"), ("egypt-river", "nine"), ("egypt-river", "mile"),
    ("egypt-river", "kyle"), ("largest-planet", "juniper"), ("france-capital", "parish"),
    ("largest-ocean", "pacifist"), ("red-planet", "bars"), ("japan-capital", "Kyoto."),
])
def test_rejected(qid, text):
    assert not ok(qid, text)


def test_no_wrong_clip_answer_is_accepted():
    """Every wrong answer the simulator says, bare and phrased, scores wrong."""
    with open(ROOT / "questions" / "general_clips.json", encoding="utf-8") as f:
        clips = json.load(f)
    for qid, q in QUESTIONS.items():
        for wrong in clips[qid]["wrong"]:
            for text in (wrong, f"I think it's {wrong}", f"The answer is {wrong}"):
                assert not ok(qid, text), (qid, text)
        for right in clips[qid]["say"]:
            assert ok(qid, right) and ok(qid, f"{right}, I think"), (qid, right)


def test_edit_distance():
    assert edit_distance("paris", "pairs") == 1  # transposition counts once
    assert edit_distance("paris", "paris") == 0
    assert edit_distance("gino", "piano") == 2
    assert edit_distance("", "abc") == 3


def test_points_scale_with_speed():
    assert points_for(0, 20) == 100
    assert points_for(10, 20) == 75
    assert points_for(20, 20) == 50
    assert points_for(25, 20) == 50   # late final (after the round ended): still counts
    assert points_for(-1, 20) == 100
    q = QUESTIONS["france-capital"]
    assert score(q, "Paris.", 5, 20) == (True, 88)
    assert score(q, "London.", 1, 20) == (False, 0)
