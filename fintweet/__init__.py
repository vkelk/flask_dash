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
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",    # hash-tags
    r"(?:\$[A-Za-z0-9]{1,5}\b)",    # cash-tags
    r'(?:http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+)',    # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',    # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",    # words with - and '
    r'(?:[\w_]+)',    # other words
    r'(?:\S)'    # anything else
]
TOKENS_RE = re.compile(r'(' + '|'.join(REGEX_STR) + ')', re.VERBOSE | re.IGNORECASE)
EMOTICON_RE = re.compile(r'^' + EMOTICONS_STR + '$', re.VERBOSE | re.IGNORECASE)
