import re

EMOTICONS_STR = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
REGEX_STR = [
    EMOTICONS_STR,
    r'<[^>]+>',    # HTML tags
    r'(?:@[\w_]+)',    # @-mentions
    r"(?:\#\w*[a-zA-Z]+\w*)",    # hash-tags
    r"(?:\$[A-Za-z0-9]{1,5}\b)",    # cash-tags
    r'(?:http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+)',    # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',    # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",    # words with - and '
    r'(?:[\w_]+)',    # other words
    r'(?:\S)'    # anything else
]
TOKENS_RE = re.compile(r'(' + '|'.join(REGEX_STR) + ')', re.VERBOSE | re.IGNORECASE)
EMOTICON_RE = re.compile(r'^' + EMOTICONS_STR + '$', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return TOKENS_RE.findall(s)


def preprocess(s, lowercase=False, upercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if TOKENS_RE.search(token) else token.lower() for token in tokens]
    if upercase:
        tokens = [token.upper() for token in tokens]
    return tokens
