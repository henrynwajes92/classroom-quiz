"""Answer scoring (CQ-7): normalise the transcript, match it, award speed points.

Normalising (``normalize``), applied to transcripts and to the accepted answers
alike (``questions.load_questions``):
- lowercase; "it's" / "that's" / "answer's" become "... is"; other
  punctuation goes (apostrophes inside words are dropped: "giraffe's" ->
  "giraffes", "I'd" -> "id");
- hesitations anywhere ("um", "uh", "er", "hmm", ...);
- lead-ins, repeatedly: "the answer is", "the answers" (gen1's spelling of
  "the answer's"), "my answer is", "I think (it is)", "I guess", "I'd say",
  "it is"/"its", "that is", "maybe", "probably", "definitely", "well",
  "okay", "oh", ...;
- trailing "I think", "I guess", "I believe", "maybe", "probably";
- leading articles "the", "a", "an" (so "a piano" and "the piano" -> "piano").
So "The answer's eight. I think." -> "eight", "I think It's the Pacific." ->
"pacific", "Um. Paris." -> "paris". A bare "A" (gen1 mishearing "eight")
becomes "" and never matches.

Matching (``is_correct``): exact match against an accepted answer, else a
fuzzy match word by word. The transcript must have as many words as the
answer, and each word must equal the answer's word or be a near miss of it:
- only answer words of 5+ letters get a near miss, with at most 1 edit
  (2 edits for 8+ letters), counting a swap of two neighbouring letters as
  one edit;
- the first letter must match, and for words under 8 letters the last too;
- never for numbers (digits or number words: "6 legs", "eighty" stay wrong).
So "pairs" and "Parris" count for "Paris", "William Shakespear" for
"William Shakespeare"; "planet Earth" (Mars), "Niger river" (Nile),
"money" (honey), "fellow" (yellow), "parish" (Paris), "juniper" (Jupiter)
don't. Short answers ("mars", "nile", "ate") are exact-only. Gen1's worst
mishearings of bare short answers ("Civic" for "Pacific", "Gino" for
"piano") are too far to accept; the phone hint ("answer in a few words") and
recognition context address those.

Points (``points_for``): a correct answer scores
``50 + 50 * (1 - answer_seconds / time_limit)``, rounded, with the fraction
clamped to 0..1: 100 for an instant answer, 75 at half time, 50 at (or
after, for a late final) the time limit. ``answer_seconds`` is question start
-> answer reported to the room (Transcribe's final plus the bridge's
``FINAL_QUIET_GAP``), so recognition latency counts against the player too.
Wrong answers score 0.
"""
import re

POINTS_MIN, POINTS_MAX = 50, 100  # correct answer at the time limit / instantly

FILLERS = {"um", "umm", "uh", "uhh", "uhm", "er", "erm", "hmm", "hm", "ah", "mm"}
LEAD_INS = [p.split() for p in (
    "the answer is", "the answers", "my answer is", "answer is", "i think it is", "i think", "i guess",
    "i believe", "i would say", "id say", "it is", "its", "that is", "maybe", "probably",
    "definitely", "well", "okay", "ok", "oh", "so the answer is", "is it")]
TRAILERS = [p.split() for p in ("i think", "i guess", "i believe", "maybe", "probably")]
ARTICLES = {"the", "a", "an"}
NUMBER_WORDS = set((
    "zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen "
    "fifteen sixteen seventeen eighteen nineteen twenty thirty forty fifty sixty seventy "
    "eighty ninety hundred thousand million").split())


def normalize(text: str) -> str:
    text = text.lower().replace("’", "'")
    text = re.sub(r"\b(it|that|answer)'s\b", r"\1 is", text)
    text = re.sub(r"(\w)'(\w)", r"\1\2", text)  # other apostrophes: giraffe's -> giraffes
    words = [w for w in re.sub(r"[^\w\s]", " ", text).split() if w not in FILLERS]
    changed = True
    while changed and words:
        changed = False
        for p in LEAD_INS:
            if words[:len(p)] == p:
                words, changed = words[len(p):], True
        for p in TRAILERS:
            if len(words) > len(p) and words[-len(p):] == p:
                words, changed = words[:-len(p)], True
        if words and words[0] in ARTICLES:
            words, changed = words[1:], True
    return " ".join(words)


def edit_distance(a: str, b: str) -> int:
    """Levenshtein distance, counting a swap of two neighbouring letters as one edit."""
    prev2, prev = None, list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        cur = [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] != b[j - 1]))
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                cur[j] = min(cur[j], prev2[j - 2] + 1)
        prev2, prev = prev, cur
    return prev[-1]


def max_edits(word: str) -> int:
    """Edits allowed for a near miss of this answer word (0: exact only)."""
    if not word.isalpha() or word in NUMBER_WORDS:
        return 0
    return 2 if len(word) >= 8 else 1 if len(word) >= 5 else 0


def near_miss(heard: str, word: str) -> bool:
    limit = max_edits(word)
    if not limit or not heard.isalpha() or heard in NUMBER_WORDS or heard[0] != word[0]:
        return False
    if len(word) < 8 and heard[-1] != word[-1]:
        return False
    return abs(len(heard) - len(word)) <= limit and edit_distance(heard, word) <= limit


def is_correct(answers: list[str], transcript: str) -> bool:
    """``answers`` are already normalised (see questions.load_questions)."""
    heard = normalize(transcript)
    if not heard:
        return False
    if heard in answers:
        return True
    words = heard.split()
    for a in answers:
        aw = a.split()
        if len(aw) == len(words) and all(h == w or near_miss(h, w) for h, w in zip(words, aw)):
            return True
    return False


def points_for(answer_seconds: float, time_limit: float) -> int:
    left = 1 - answer_seconds / time_limit if time_limit > 0 else 0
    return round(POINTS_MIN + (POINTS_MAX - POINTS_MIN) * min(1.0, max(0.0, left)))


def score(question, transcript: str, answer_seconds: float, time_limit: float) -> tuple[bool, int]:
    """Return (correct, points) for a questions.Question.
    answer_seconds: question start -> answer reported (includes FINAL_QUIET_GAP)."""
    correct = is_correct(question.answers, transcript)
    return correct, points_for(answer_seconds, time_limit) if correct else 0
