"""Local reference parsing utilities copied from the legacy hallucinator parser."""

from __future__ import annotations

import re
from typing import Any

GREEK_TRANSLITERATIONS = {
    # Lowercase
    'α': 'alpha', 'β': 'beta', 'γ': 'gamma', 'δ': 'delta', 'ε': 'epsilon',
    'ζ': 'zeta', 'η': 'eta', 'θ': 'theta', 'ι': 'iota', 'κ': 'kappa',
    'λ': 'lambda', 'μ': 'mu', 'ν': 'nu', 'ξ': 'xi', 'ο': 'o',
    'π': 'pi', 'ρ': 'rho', 'σ': 'sigma', 'ς': 'sigma',
    'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi', 'ψ': 'psi', 'ω': 'omega',
    # Uppercase
    'Α': 'alpha', 'Β': 'beta', 'Γ': 'gamma', 'Δ': 'delta', 'Ε': 'epsilon',
    'Ζ': 'zeta', 'Η': 'eta', 'Θ': 'theta', 'Ι': 'iota', 'Κ': 'kappa',
    'Λ': 'lambda', 'Μ': 'mu', 'Ν': 'nu', 'Ξ': 'xi', 'Ο': 'o',
    'Π': 'pi', 'Ρ': 'rho', 'Σ': 'sigma',
    'Τ': 'tau', 'Υ': 'upsilon', 'Φ': 'phi', 'Χ': 'chi', 'Ψ': 'psi', 'Ω': 'omega',
}

def transliterate_greek(text):
    """Transliterate Greek letters to ASCII equivalents."""
    for greek, latin in GREEK_TRANSLITERATIONS.items():
        text = text.replace(greek, latin)
    return text

DIACRITIC_COMPOSITIONS = {
    # Umlaut/diaeresis (¨)
    ('¨', 'A'): 'Ä', ('¨', 'a'): 'ä',
    ('¨', 'E'): 'Ë', ('¨', 'e'): 'ë',
    ('¨', 'I'): 'Ï', ('¨', 'i'): 'ï',
    ('¨', 'O'): 'Ö', ('¨', 'o'): 'ö',
    ('¨', 'U'): 'Ü', ('¨', 'u'): 'ü',
    ('¨', 'Y'): 'Ÿ', ('¨', 'y'): 'ÿ',
    # Acute accent (´)
    ('´', 'A'): 'Á', ('´', 'a'): 'á',
    ('´', 'E'): 'É', ('´', 'e'): 'é',
    ('´', 'I'): 'Í', ('´', 'i'): 'í',
    ('´', 'O'): 'Ó', ('´', 'o'): 'ó',
    ('´', 'U'): 'Ú', ('´', 'u'): 'ú',
    ('´', 'N'): 'Ń', ('´', 'n'): 'ń',
    ('´', 'C'): 'Ć', ('´', 'c'): 'ć',
    ('´', 'S'): 'Ś', ('´', 's'): 'ś',
    ('´', 'Z'): 'Ź', ('´', 'z'): 'ź',
    ('´', 'Y'): 'Ý', ('´', 'y'): 'ý',
    # Grave accent (`)
    ('`', 'A'): 'À', ('`', 'a'): 'à',
    ('`', 'E'): 'È', ('`', 'e'): 'è',
    ('`', 'I'): 'Ì', ('`', 'i'): 'ì',
    ('`', 'O'): 'Ò', ('`', 'o'): 'ò',
    ('`', 'U'): 'Ù', ('`', 'u'): 'ù',
    # Tilde (~, ˜)
    ('~', 'A'): 'Ã', ('~', 'a'): 'ã', ('˜', 'A'): 'Ã', ('˜', 'a'): 'ã',
    ('~', 'N'): 'Ñ', ('~', 'n'): 'ñ', ('˜', 'N'): 'Ñ', ('˜', 'n'): 'ñ',
    ('~', 'O'): 'Õ', ('~', 'o'): 'õ', ('˜', 'O'): 'Õ', ('˜', 'o'): 'õ',
    # Caron/háček (ˇ)
    ('ˇ', 'C'): 'Č', ('ˇ', 'c'): 'č',
    ('ˇ', 'S'): 'Š', ('ˇ', 's'): 'š',
    ('ˇ', 'Z'): 'Ž', ('ˇ', 'z'): 'ž',
    ('ˇ', 'E'): 'Ě', ('ˇ', 'e'): 'ě',
    ('ˇ', 'R'): 'Ř', ('ˇ', 'r'): 'ř',
    ('ˇ', 'N'): 'Ň', ('ˇ', 'n'): 'ň',
    # Circumflex (^)
    ('^', 'A'): 'Â', ('^', 'a'): 'â',
    ('^', 'E'): 'Ê', ('^', 'e'): 'ê',
    ('^', 'I'): 'Î', ('^', 'i'): 'î',
    ('^', 'O'): 'Ô', ('^', 'o'): 'ô',
    ('^', 'U'): 'Û', ('^', 'u'): 'û',
}

SPACE_BEFORE_DIACRITIC_PATTERN = re.compile(r'([A-Za-z])\s+([¨´`~˜ˇ^])')

SEPARATED_DIACRITIC_PATTERN = re.compile(r'([¨´`~˜ˇ^])\s*([A-Za-z])')

def fix_separated_diacritics(text):
    """Fix separated diacritics from PDF extraction.

    Converts patterns like "B ¨UNZ" to "BÜNZ" and "R´enyi" to "Rényi".
    """
    # Step 1: Remove spaces between a letter and a diacritic (like "B ¨" -> "B¨")
    text = SPACE_BEFORE_DIACRITIC_PATTERN.sub(r'\1\2', text)

    # Step 2: Compose diacritic + letter into single character
    def replace_match(m):
        diacritic = m.group(1)
        letter = m.group(2)
        composed = DIACRITIC_COMPOSITIONS.get((diacritic, letter))
        if composed:
            return composed
        # If no mapping, just return the letter (diacritic gets dropped)
        return letter

    return SEPARATED_DIACRITIC_PATTERN.sub(replace_match, text)

AUTHOR_LIST_PATTERNS = [
    # SURNAME, I., SURNAME, I., AND SURNAME, I.
    re.compile(r'^[A-Z]{2,}\s*,\s*[A-Z]\.\s*,\s*[A-Z]{2,}\s*,\s*[A-Z]\.'),
    # SURNAME, I., AND SURNAME, I.,
    re.compile(r'^[A-Z]{2,}\s*,\s*[A-Z]\.\s*,?\s*AND\s+[A-Z]'),
    # SURNAME, AND I. SURNAME (like "HORESH, AND M. RIABZEV")
    re.compile(r'^[A-Z]{2,}\s*,\s*AND\s+[A-Z]\.\s*[A-Z]'),
    # TWO WORD SURNAME AND I. SURNAME (like "EL HOUSNI AND G. BOTREL")
    re.compile(r'^[A-Z]{2,}\s+[A-Z]{2,}\s+AND\s+[A-Z]\.\s*[A-Z]'),
    # SURNAME AND SURNAME,
    re.compile(r'^[A-Z]{2,}\s+AND\s+[A-Z]\.\s*[A-Z]{2,}\s*,'),
    # Broken umlaut + author pattern: B ¨UNZ, P. CAMACHO
    re.compile(r'^[A-Z]\s*[¨´`]\s*[A-Z]+\s*,\s*[A-Z]\.'),
    # Short initials followed by name list: "AL, Andrew Ahn, Nic Becker, Stephanie" (OpenAI-style)
    # Requires at least two full names after initials to avoid false positives like "AI, Machine Learning,"
    re.compile(r'^[A-Z]{1,3},\s+[A-Z][a-z]+\s+[A-Z][a-z]+,\s+[A-Z][a-z]+\s+[A-Z][a-z]+'),
    # NeurIPS/ML style: "I. Surname, I. G. Surname, and I. Surname" (mixed case surnames)
    # Requires at least two "I. Surname" patterns with "and" to confirm it's an author list
    # e.g., "B. Hassibi, D. G. Stork, and G. J. Wolff"
    re.compile(r'^[A-Z]\.(?:\s*[A-Z]\.)?\s+[A-Z][a-z]+,\s+[A-Z]\.(?:\s*[A-Z]\.)?\s+[A-Z][a-z]+,\s+and\s+[A-Z]\.', re.IGNORECASE),
]

VENUE_ONLY_PATTERNS = [
    # SIAM/IEEE/ACM Journal/Transactions/Review
    re.compile(r'^(?:SIAM|IEEE|ACM|PNAS)\s+(?:Journal|Transactions|Review)', re.IGNORECASE),
    # Journal/Transactions/Proceedings of/on
    re.compile(r'^(?:Journal|Transactions|Proceedings)\s+(?:of|on)\s+', re.IGNORECASE),
    # Advances in Neural Information Processing Systems
    re.compile(r'^Advances\s+in\s+Neural', re.IGNORECASE),
]

NON_REFERENCE_PATTERNS = [
    # NeurIPS checklist bullet points
    re.compile(r'^[•\-]\s+(?:The answer|Released models|If you are using)', re.IGNORECASE),
    # Acknowledgments
    re.compile(r'^We gratefully acknowledge', re.IGNORECASE),
]

VENUE_AFTER_PUNCTUATION_PATTERN = re.compile(
    r'[?!]\s+(?:International|Proceedings|Conference|Workshop|Symposium|Association|'
    r'The\s+\d{4}\s+Conference|Nations|Annual|IEEE|ACM|USENIX|AAAI|NeurIPS|ICML|ICLR|'
    r'CVPR|ICCV|ECCV|ACL|EMNLP|NAACL)'
)

def is_likely_author_list(text):
    """Check if text looks like an author list instead of a title.

    Returns True if the text matches common author list patterns.
    This is used to reject bad title extractions.
    """
    for pattern in AUTHOR_LIST_PATTERNS:
        if pattern.match(text):
            return True
    return False

def is_venue_only(text):
    """Check if text is just a venue/journal name, not a paper title.

    Returns True if the text matches venue-only patterns.
    """
    for pattern in VENUE_ONLY_PATTERNS:
        if pattern.match(text):
            return True
    return False

def is_non_reference_content(text):
    """Check if text is non-reference content (checklists, acknowledgments, etc.).

    Returns True if the text matches non-reference patterns.
    """
    for pattern in NON_REFERENCE_PATTERNS:
        if pattern.match(text):
            return True
    return False

def truncate_title_at_venue(title):
    """Truncate title if it contains venue name after ?/! punctuation.

    Some reference formats don't have proper delimiters between title and venue,
    especially when the title ends with ? or !. This function detects and removes
    the venue portion.

    Returns the truncated title (keeping the ?/!) or original if no venue found.
    """
    match = VENUE_AFTER_PUNCTUATION_PATTERN.search(title)
    if match:
        # Keep everything up to and including the ?/!
        return title[:match.start() + 1].strip()
    return title

def extract_doi(text):
    """Extract DOI from reference text.

    Handles formats like:
    - 10.1234/example
    - doi:10.1234/example
    - https://doi.org/10.1234/example
    - http://dx.doi.org/10.1234/example
    - DOI: 10.1234/example

    Also handles DOIs split across lines (common in PDFs).

    Returns the DOI string (e.g., "10.1234/example") or None if not found.
    """
    # First, fix DOIs that are split across lines (apply to all text before pattern matching)
    # Note: Allow parentheses in DOI patterns (e.g., 10.1016/0021-9681(87)90171-8)
    # Pattern 1: DOI ending with a period followed by newline and 3+ digits
    # e.g., "10.1145/3442381.\n3450048" -> "10.1145/3442381.3450048"
    # e.g., "10.48550/arXiv.2404.\n06011" -> "10.48550/arXiv.2404.06011"
    # Requires 3+ digits to avoid joining sentence periods with short page numbers (e.g., ".\n18")
    text_fixed = re.sub(r'(10\.\d{4,}/[^\s\]>,]+\.)\s*\n\s*(\d{3,})', r'\1\2', text)

    # Pattern 1b: DOI ending with digits followed by newline and DOI continuation
    # e.g., "10.1109/SP40000.20\n20.00038" -> "10.1109/SP40000.2020.00038"
    # e.g., "10.1145/2884781.2884\n807" -> "10.1145/2884781.2884807"
    # e.g., "10.1109/TSE.20\n18.2884955" -> "10.1109/TSE.2018.2884955"
    # Continuation must look like DOI content: digits optionally followed by .digits
    text_fixed = re.sub(r'(10\.\d{4,}/[^\s\]>,]+\d)\s*\n\s*(\d+(?:\.\d+)*)', r'\1\2', text_fixed)

    # Pattern 2: DOI ending with a dash followed by newline and continuation
    # e.g., "10.2478/popets-\n2019-0037" -> "10.2478/popets-2019-0037"
    text_fixed = re.sub(r'(10\.\d{4,}/[^\s\]>,]+-)\s*\n\s*(\S+)', r'\1\2', text_fixed)

    # Pattern 3: URL split across lines - doi.org URL followed by newline and DOI continuation
    # e.g., "https://doi.org/10.48550/arXiv.2404.\n06011"
    text_fixed = re.sub(r'(https?://(?:dx\.)?doi\.org/10\.\d{4,}/[^\s\]>,]+\.)\s*\n\s*(\d+)', r'\1\2', text_fixed, flags=re.IGNORECASE)

    # Pattern 3b: URL split mid-number
    text_fixed = re.sub(r'(https?://(?:dx\.)?doi\.org/10\.\d{4,}/[^\s\]>,]+\d)\s*\n\s*(\d[^\s\]>,]*)', r'\1\2', text_fixed, flags=re.IGNORECASE)

    # Priority 1: Extract from URL format (most reliable - clear boundaries)
    # Matches https://doi.org/... or http://dx.doi.org/... or http://doi.org/...
    # Allow parentheses in DOI (e.g., 10.1016/0021-9681(87)90171-8)
    url_pattern = r'https?://(?:dx\.)?doi\.org/(10\.\d{4,}/[^\s\]>},]+)'
    url_match = re.search(url_pattern, text_fixed, re.IGNORECASE)
    if url_match:
        doi = url_match.group(1)
        # Clean trailing punctuation and fix unbalanced parentheses
        doi = _clean_doi(doi)
        return doi

    # Priority 2: DOI pattern without URL prefix
    # 10.XXXX/suffix where suffix can contain various characters including parentheses
    # The suffix ends at whitespace, or common punctuation at end of reference
    # Allow parentheses (e.g., 10.1016/0021-9681(87)90171-8)
    doi_pattern = r'10\.\d{4,}/[^\s\]>},]+'

    match = re.search(doi_pattern, text_fixed)
    if match:
        doi = match.group(0)
        # Clean trailing punctuation and fix unbalanced parentheses
        doi = _clean_doi(doi)
        return doi
    return None

def _clean_doi(doi):
    """Clean a DOI string by removing trailing punctuation and unbalanced parentheses.

    DOIs can legitimately contain parentheses (e.g., 10.1016/0021-9681(87)90171-8),
    but trailing unbalanced ')' are likely reference delimiters, not part of the DOI.
    """
    # First, strip common trailing punctuation
    doi = doi.rstrip('.,;:')

    # Handle unbalanced parentheses at the end
    # If DOI ends with ')' and parens are unbalanced, strip trailing ')'
    while doi.endswith(')'):
        open_count = doi.count('(')
        close_count = doi.count(')')
        if close_count > open_count:
            doi = doi[:-1].rstrip('.,;:')
        else:
            break

    # Similarly for brackets and braces (less common but possible)
    while doi.endswith(']') and doi.count(']') > doi.count('['):
        doi = doi[:-1].rstrip('.,;:')
    while doi.endswith('}') and doi.count('}') > doi.count('{'):
        doi = doi[:-1].rstrip('.,;:')

    return doi

def extract_arxiv_id(text):
    """Extract arXiv ID from reference text.

    Handles formats like:
    - arXiv:2301.12345
    - arXiv:2301.12345v1
    - arxiv.org/abs/2301.12345
    - arXiv:hep-th/9901001 (old format)
    - arXiv preprint arXiv:2301.12345

    Also handles IDs split across lines (common in PDFs).

    Returns the arXiv ID string (e.g., "2301.12345") or None if not found.
    """
    # Fix IDs split across lines
    # e.g., "arXiv:2301.\n12345" -> "arXiv:2301.12345"
    text_fixed = re.sub(r'(arXiv:\d{4}\.)\s*\n\s*(\d+)', r'\1\2', text, flags=re.IGNORECASE)
    # e.g., "arxiv.org/abs/2301.\n12345" -> "arxiv.org/abs/2301.12345"
    text_fixed = re.sub(r'(arxiv\.org/abs/\d{4}\.)\s*\n\s*(\d+)', r'\1\2', text_fixed, flags=re.IGNORECASE)

    # New format: YYMM.NNNNN (with optional version)
    # e.g., arXiv:2301.12345, arXiv:2301.12345v2
    new_format = re.search(r'arXiv[:\s]+(\d{4}\.\d{4,5}(?:v\d+)?)', text_fixed, re.IGNORECASE)
    if new_format:
        return new_format.group(1)

    # URL format: arxiv.org/abs/YYMM.NNNNN
    url_format = re.search(r'arxiv\.org/abs/(\d{4}\.\d{4,5}(?:v\d+)?)', text_fixed, re.IGNORECASE)
    if url_format:
        return url_format.group(1)

    # Old format: category/YYMMNNN (e.g., hep-th/9901001)
    old_format = re.search(r'arXiv[:\s]+([a-z-]+/\d{7}(?:v\d+)?)', text_fixed, re.IGNORECASE)
    if old_format:
        return old_format.group(1)

    # URL old format
    url_old_format = re.search(r'arxiv\.org/abs/([a-z-]+/\d{7}(?:v\d+)?)', text_fixed, re.IGNORECASE)
    if url_old_format:
        return url_old_format.group(1)

    return None

COMPOUND_SUFFIXES = {
    'centered', 'based', 'driven', 'aware', 'oriented', 'specific', 'related',
    'dependent', 'independent', 'like', 'free', 'friendly', 'rich', 'poor',
    'scale', 'level', 'order', 'class', 'type', 'style', 'wise', 'fold',
    'shot', 'step', 'time', 'world', 'source', 'domain', 'task', 'modal',
    'intensive', 'efficient', 'agnostic', 'invariant', 'sensitive', 'grained',
    'agent', 'site',
}

def fix_hyphenation(text):
    """Fix hyphenation from PDF line breaks while preserving compound words.

    - 'detec- tion' or 'detec-\\ntion' → 'detection' (syllable break)
    - 'human- centered' or 'human-\\ncentered' → 'human-centered' (compound word)
    """
    text = re.sub(r'\bstate-\s+of-the-art\b', 'state-of-the-art', text, flags=re.IGNORECASE)
    text = re.sub(r'\bagent-\s+generated\b', 'agent-generated', text, flags=re.IGNORECASE)
    text = re.sub(r'\bai-\s+generated\b', 'ai-generated', text, flags=re.IGNORECASE)

    def replace_hyphen(match):
        before = match.group(1)  # character before hyphen
        after_char = match.group(2)  # first character after hyphen
        after_rest = match.group(3)  # rest of word after hyphen

        after_word = after_char + after_rest

        # If the part before hyphen ends with a digit, keep the hyphen
        # These are product/model names like "Qwen2-VL", "GPT-4-turbo", "BERT-base"
        if before.isdigit():
            return f'{before}-{after_word}'

        # If the word after hyphen is a common compound suffix, keep the hyphen
        after_lower = after_word.lower()
        for suffix in COMPOUND_SUFFIXES:
            if after_lower == suffix or after_lower.startswith(suffix + ' ') or after_lower.startswith(suffix + ','):
                return f'{before}-{after_word}'
        # Check if the full word matches a suffix
        if after_lower.rstrip('.,;:') in COMPOUND_SUFFIXES:
            return f'{before}-{after_word}'
        # Otherwise, it's likely a syllable break - remove hyphen
        return f'{before}{after_word}'

    # Fix hyphen followed by space or newline, capturing the full word after
    text = re.sub(r'(\w)-\s+(\w)(\w*)', replace_hyphen, text)
    text = re.sub(r'(\w)- (\w)(\w*)', replace_hyphen, text)
    return text

def expand_ligatures(text):
    """Expand common typographic ligatures found in PDFs."""
    ligatures = {
        '\ufb00': 'ff',   # ﬀ
        '\ufb01': 'fi',   # ﬁ
        '\ufb02': 'fl',   # ﬂ
        '\ufb03': 'ffi',  # ﬃ
        '\ufb04': 'ffl',  # ﬄ
        '\ufb05': 'st',   # ﬅ (long s + t)
        '\ufb06': 'st',   # ﬆ
    }
    for lig, expanded in ligatures.items():
        text = text.replace(lig, expanded)
    return text

def find_references_section(text):
    """Locate the references section in the document text."""
    # Common reference section headers
    headers = [
        r'\n\s*References\s*\n',
        r'\n\s*REFERENCES\s*\n',
        r'\n\s*Bibliography\s*\n',
        r'\n\s*BIBLIOGRAPHY\s*\n',
        r'\n\s*Works Cited\s*\n',
    ]

    for pattern in headers:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            ref_start = match.end()
            # Find end markers (Appendix, Acknowledgments, Ethics, etc.)
            # Also detect single-letter appendix markers (A Proofs, B Methods)
            # and TOC dot leaders (. . . . . page numbers)
            # End markers with appropriate flags
            # Case-insensitive for keywords, case-sensitive for appendix letter pattern
            end_markers_icase = [
                r'\n\s*(?:Appendix|Acknowledge?ments?|Supplementary|Ethics\s+Statement|Ethical\s+Considerations|Broader\s+Impact|Paper\s+Checklist|Checklist|Contents)\b',
                r'(?:\.\s*){5,}',  # TOC dot leaders (5+ dots with optional spaces)
            ]
            # Case-sensitive: single letter + capitalized word (A Proofs, B Methods)
            # Must NOT use IGNORECASE or it will match "A dataset..." etc.
            end_markers_case = [
                r'\n\s*[A-Z]\s+[A-Z][a-zA-Z-]+(?:\s+[a-zA-Z-]+)*\s*\n',
            ]
            ref_end = len(text)
            for end_marker in end_markers_icase:
                end_match = re.search(end_marker, text[ref_start:], re.IGNORECASE)
                if end_match:
                    ref_end = min(ref_end, ref_start + end_match.start())
            for end_marker in end_markers_case:
                end_match = re.search(end_marker, text[ref_start:])
                if end_match:
                    ref_end = min(ref_end, ref_start + end_match.start())

            ref_text = text[ref_start:ref_end]
            # Strip running headers (common in ACM papers)
            ref_text = strip_running_headers(ref_text)
            return ref_text

    # Fallback: try last 30% of document
    cutoff = int(len(text) * 0.7)
    return strip_running_headers(text[cutoff:])

def strip_running_headers(text):
    """Remove running headers that appear at page boundaries in references.

    ACM papers have headers like:
    - "CONFERENCE 'YY, Month DD–DD, YYYY, City, Country"
    - "Paper Title Here"
    - "A. Author, B. Author, and C. Author"

    Math papers have headers like:
    - "HODGE THEORY OF SECANT VARIETIES" (all caps title)
    - "99" (page numbers on their own line)

    These get mixed into references when they span page boundaries.
    """
    # Pattern for ACM-style venue headers
    # e.g., "ASIA CCS '26, June 01–05, 2026, Bangalore, India"
    # Matches: CONF_NAME 'YY, Month DD–DD, YYYY, Location
    # Note: Uses Unicode right single quote (U+2019) and en-dash (U+2013)
    venue_pattern = r"^[A-Z][A-Z\s&]+\s*['\u2019]\d{2},\s+[A-Z][a-z]+\s+\d{1,2}[\u2013\-]+\d{1,2},\s+\d{4},\s+[A-Z][A-Za-z\s,]+$"

    # Pattern for abbreviated author headers
    # e.g., "O.A Akanji, M. Egele, and G. Stringhini"
    author_header_pattern = r'^[A-Z]\.?[A-Z]?\s+[A-Z][a-z]+(?:,\s+[A-Z]\.?\s*[A-Z]?\.?\s*[A-Z][a-z]+)*(?:,?\s+and\s+[A-Z]\.?\s*[A-Z]?\.?\s*[A-Z][a-z]+)?$'

    # Pattern for math paper running headers (ALL CAPS title with at least 3 words)
    # e.g., "HODGE THEORY OF SECANT VARIETIES"
    math_title_header_pattern = r'^[A-Z][A-Z\s\-]+$'

    # Pattern for standalone page numbers (math papers often have these)
    # e.g., "99" or "123"
    page_number_pattern = r'^\d{1,4}$'

    lines = text.split('\n')
    filtered_lines = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Check if this line matches venue pattern
        if re.match(venue_pattern, line):
            # Skip this line and check adjacent lines for paper title/authors
            # Check previous line (might be paper title)
            if filtered_lines and len(filtered_lines[-1].strip()) > 20:
                prev_line = filtered_lines[-1].strip()
                # If previous line looks like a title (not a reference continuation), remove it
                if not re.match(r'^\[\d+\]', prev_line) and not re.match(r'^\d+\.', prev_line):
                    # Check if it's not a normal sentence (titles usually don't end with period followed by venue)
                    if not prev_line.endswith('.') or 'doi:' not in lines[i-1] if i > 0 else True:
                        filtered_lines.pop()
            # Skip the venue line
            i += 1
            continue

        # Check if this line matches author header pattern
        if re.match(author_header_pattern, line) and len(line) < 100:
            # This is likely a running header with authors, skip it
            i += 1
            continue

        # Check for math paper ALL CAPS title headers (at least 3 words, all caps)
        if re.match(math_title_header_pattern, line) and len(line.split()) >= 3 and len(line) > 15:
            # This is likely a math paper title running header, skip it
            i += 1
            continue

        # Check for standalone page numbers
        if re.match(page_number_pattern, line):
            # This is likely a page number, skip it
            i += 1
            continue

        filtered_lines.append(lines[i])
        i += 1

    return '\n'.join(filtered_lines)

def segment_references(ref_text):
    """Split references section into individual references."""
    # Preprocess: detect and truncate at reference section boundary
    # Some PDFs include appendix/supplementary material after references
    # Look for patterns that indicate end of references section
    boundary_patterns = [
        # Appendix headers - explicit "Appendix" keyword
        r'\n\s*(?:APPENDIX|Appendix)\s*[A-Z]?\s*[\n:.]',
        r'\n\s*(?:SUPPLEMENTARY|Supplementary)\s+(?:MATERIAL|Material|INFORMATION|Information)',
        # Appendix section header: single letter on its own line followed by section title
        # e.g., "\nA\nDetailed Benchmark Results"
        r'\n\s*[A-Z]\s*\n\s*(?:Additional|Detailed|Extended|Supplemental|Proof|Experimental|Implementation|Benchmark|Dataset|Ablation|Hyperparameter)',
        # Section headers like "A. Additional Results" or "A Additional Results" (same line)
        r'\n\s*[A-Z]\s*[\.:]?\s+(?:Additional|Detailed|Extended|Supplemental|Proof|Experimental|Implementation|Benchmark|Dataset|Ablation|Hyperparameter)\s+',
        # Mathematical proof section - standalone equation numbers like "(17)" on their own line
        # followed by mathematical content (equations use = sign)
        r'\n\s*\(\d{1,3}\)\s*\n[^\n]*=',
    ]

    earliest_boundary = len(ref_text)
    for pattern in boundary_patterns:
        match = re.search(pattern, ref_text)
        if match and match.start() < earliest_boundary:
            # Ensure we have at least some content before truncating
            if match.start() > 500:  # Minimum 500 chars of references
                earliest_boundary = match.start()

    if earliest_boundary < len(ref_text):
        ref_text = ref_text[:earliest_boundary].strip()

    # Try IEEE style: [1], [2], etc.
    ieee_pattern = r'(?:^|\n)\s*\[(\d+)\]\s*'
    ieee_matches = list(re.finditer(ieee_pattern, ref_text))

    if len(ieee_matches) >= 3:
        refs = []
        for i, match in enumerate(ieee_matches):
            start = match.end()
            end = ieee_matches[i + 1].start() if i + 1 < len(ieee_matches) else len(ref_text)
            ref_content = ref_text[start:end].strip()
            if ref_content:
                refs.append(ref_content)
        return refs

    # Try alphabetic citation keys: [ACGH20], [CCY20], etc. (common in crypto/theory papers)
    # Pattern: uppercase letters (author initials) followed by 2-4 digits (year)
    # Also handles lowercase variants like [ABC+20] or [ABCea20]
    alpha_cite_pattern = r'\n\s*\[([A-Za-z+]+\d{2,4}[a-z]?)\]\s*'
    alpha_matches = list(re.finditer(alpha_cite_pattern, ref_text))

    if len(alpha_matches) >= 3:
        refs = []
        for i, match in enumerate(alpha_matches):
            start = match.end()
            end = alpha_matches[i + 1].start() if i + 1 < len(alpha_matches) else len(ref_text)
            ref_content = ref_text[start:end].strip()
            if ref_content:
                refs.append(ref_content)
        return refs

    # Try numbered list style: 1., 2., etc.
    # Validate that numbers are sequential starting from 1 (not years like 2019. or page numbers)
    # Use (?:^|\n) to also match at start of string (reference 1 has no preceding newline)
    numbered_pattern = r'(?:^|\n)\s*(\d+)\.\s+'
    numbered_matches = list(re.finditer(numbered_pattern, ref_text))

    if len(numbered_matches) >= 3:
        # Check if first few numbers look like sequential reference numbers (1, 2, 3...)
        first_nums = [int(m.group(1)) for m in numbered_matches[:5]]
        is_sequential = first_nums[0] == 1 and all(
            first_nums[i] == first_nums[i-1] + 1 for i in range(1, len(first_nums))
        )
        if is_sequential:
            refs = []
            for i, match in enumerate(numbered_matches):
                start = match.end()
                end = numbered_matches[i + 1].start() if i + 1 < len(numbered_matches) else len(ref_text)
                ref_content = ref_text[start:end].strip()
                if ref_content:
                    refs.append(ref_content)
            return refs

    # Try AAAI/ACM author-year style: "Surname, I.; ... Year. Title..."
    # Each reference starts with a surname (capitalized word, possibly hyphenated or two-part)
    # followed by comma and author initial(s)
    # Pattern matches: "Avalle, M.", "Camacho-collados, J.", "Del Vicario, M.", "Van Bavel, J."
    # Must be preceded by period+newline (end of previous reference) to avoid matching
    # author names that wrap to new lines mid-reference
    # Match after: lowercase letter, digit, closing paren, or 2+ uppercase letters (venue abbrevs like CSCW, CHI)
    # Single uppercase letter excluded to avoid matching author initials like "A."
    # (?!In\s) negative lookahead excludes "In Surname, I." which indicates editors, not new reference
    # Group 1 captures the prefix char(s) so we can include them in the previous reference
    # (?:\d{1,4}\n)? handles page/reference numbers on their own line between references
    # \s* after optional page number handles extra whitespace/newlines (e.g., column breaks)
    # Primary pattern: personal authors (unicode-aware for diacritics)
    aaai_pattern = r'([a-z0-9)]|[A-Z]{2})\.\n(?:\d{1,4}\n)?\s*(?!In\s)([A-Z][a-zA-Z\u00C0-\u024F]+(?:[ -][A-Za-z\u00C0-\u024F]+)?,\s+[A-Z]\.)'
    # Secondary pattern: organization authors (e.g., "European Union. 2022a.")
    aaai_org_pattern = r'([a-z0-9)]|[A-Z]{2})\.\n(?:\d{1,4}\n)?\s*(?!In\s)([A-Z][a-zA-Z\u00C0-\u024F]+(?:\s+[A-Z][a-zA-Z\u00C0-\u024F]+)+\.\s+\d{4}[a-z]?\.)'
    aaai_matches = list(re.finditer(aaai_pattern, ref_text))
    aaai_org_matches = list(re.finditer(aaai_org_pattern, ref_text))

    # Merge boundaries from both patterns, sort, deduplicate within 10 chars
    all_aaai = aaai_matches + aaai_org_matches
    if all_aaai:
        all_aaai.sort(key=lambda m: m.start())
        merged = [all_aaai[0]]
        for m in all_aaai[1:]:
            if m.start() - merged[-1].start() > 10:
                merged.append(m)
        all_aaai = merged

    if len(all_aaai) >= 3:
        refs = []
        # Handle first reference (before first match) - starts at beginning of ref_text
        # end(1) includes the consumed prefix char(s) in the previous reference
        first_ref = ref_text[:all_aaai[0].end(1)].strip()
        if first_ref and len(first_ref) > 20:
            refs.append(first_ref)
        # Handle remaining references
        for i, match in enumerate(all_aaai):
            start = match.start(2)  # Start at the author name (group 2)
            end = all_aaai[i + 1].end(1) if i + 1 < len(all_aaai) else len(ref_text)
            ref_content = ref_text[start:end].strip()
            if ref_content:
                refs.append(ref_content)
        return refs

    # Try Springer/Nature style: "Surname I, Surname I, ... (Year) Title"
    # Authors use format: Surname Initial (no comma/period between surname and initial)
    # e.g., "Abrahao S, Grundy J, Pezze M, et al (2025) Software Engineering..."
    # Each reference starts on a new line with author name and has (year) within first ~100 chars
    # Split by finding lines that look like reference starts
    lines = ref_text.split('\n')
    ref_starts = []
    current_pos = 0

    for i, line in enumerate(lines):
        # Check if line looks like a reference start:
        # - Starts with capital letter (author surname or organization)
        # - Contains (YYYY) or (YYYYa) pattern within reasonable distance
        # - Not just a page number
        if (line and
            re.match(r'^[A-Z]', line) and
            not re.match(r'^\d+$', line.strip()) and
            re.search(r'\(\d{4}[a-z]?\)', line)):
            ref_starts.append(current_pos)
        current_pos += len(line) + 1  # +1 for newline

    if len(ref_starts) >= 5:
        refs = []
        for i, start in enumerate(ref_starts):
            end = ref_starts[i + 1] if i + 1 < len(ref_starts) else len(ref_text)
            ref_content = ref_text[start:end].strip()
            # Remove trailing page number if present (standalone number at end)
            ref_content = re.sub(r'\n+\d+\s*$', '', ref_content).strip()
            if ref_content and len(ref_content) > 20:
                refs.append(ref_content)
        return refs

    # Try economics/math style: ", YYYY.\nAuthorName" (year at end, no parentheses)
    # e.g., "...pages 619–636, 2015.\nDaron Acemoglu, Ali Makhdoumi..."
    # Pattern: ends with ", YYYY." or "), YYYY." then new line starts with author name
    # Author pattern: FirstName LastName, FirstName LastName, ... or single capitalized name
    econ_pattern = r'[,)]\s*\d{4}[a-z]?\.\n+([A-Z][a-zA-Z\u00C0-\u024F]+(?:[ -][A-Za-z\u00C0-\u024F]+)*[,\s]+(?:[A-Z]\.?\s*)?[A-Z][a-zA-Z\u00C0-\u024F-]+)'
    econ_matches = list(re.finditer(econ_pattern, ref_text))

    if len(econ_matches) >= 5:
        refs = []
        # First reference: from start to first match
        first_ref = ref_text[:econ_matches[0].start() + econ_matches[0].group().index('\n')].strip()
        # Include up to the period after year
        period_pos = first_ref.rfind('.')
        if period_pos > 0:
            first_ref = first_ref[:period_pos + 1].strip()
        if first_ref and len(first_ref) > 20:
            refs.append(first_ref)
        # Remaining references: from author name to next match
        for i, match in enumerate(econ_matches):
            start = match.start(1)  # Start at the author name (group 1)
            if i + 1 < len(econ_matches):
                end_match = econ_matches[i + 1]
                end = end_match.start() + end_match.group().index('\n') + 1
            else:
                end = len(ref_text)
            ref_content = ref_text[start:end].strip()
            # Remove trailing page numbers
            ref_content = re.sub(r'\n+\d+\s*$', '', ref_content).strip()
            if ref_content and len(ref_content) > 20:
                refs.append(ref_content)
        return refs

    # Try NeurIPS/ML style: "I. Surname and I. Surname. Title. Venue, Year."
    # References use author-initial format (I. Surname or I. I. Surname)
    # Each reference ends with period, then new reference starts with initials
    # Pattern: previous ref ends with period (after year or page), newline(s), then "I. Surname"
    # Must include "and" or "," after first author to confirm it's multi-author
    # e.g., "...2020.\nC. D. Aliprantis and K. C. Border. Infinite..."
    neurips_pattern = r'(\.\s*)\n+([A-Z]\.(?:\s*[A-Z]\.)?\s+[A-Z][a-zA-Z\u00C0-\u024F-]+(?:\s+and\s+[A-Z]\.|,\s+[A-Z]\.))'
    neurips_matches = list(re.finditer(neurips_pattern, ref_text))

    if len(neurips_matches) >= 5:
        refs = []
        # First reference: from start to first match
        first_end = neurips_matches[0].start() + len(neurips_matches[0].group(1))
        first_ref = ref_text[:first_end].strip()
        if first_ref and len(first_ref) > 20:
            refs.append(first_ref)
        # Remaining references
        for i, match in enumerate(neurips_matches):
            start = match.start(2)  # Start at the author initials
            if i + 1 < len(neurips_matches):
                end = neurips_matches[i + 1].start() + len(neurips_matches[i + 1].group(1))
            else:
                end = len(ref_text)
            ref_content = ref_text[start:end].strip()
            if ref_content and len(ref_content) > 20:
                refs.append(ref_content)
        return refs

    # Fallback: split by double newlines
    paragraphs = re.split(r'\n\s*\n', ref_text)
    return [p.strip() for p in paragraphs if p.strip() and len(p.strip()) > 20]

def extract_authors_from_reference(ref_text):
    """Extract author names from a reference string.

    Handles three main formats:
    - IEEE: "J. Smith, A. Jones, and C. Williams, "Title...""
    - ACM: "FirstName LastName, FirstName LastName, and FirstName LastName. Year."
    - USENIX: "FirstName LastName and FirstName LastName. Title..."

    Returns a list of author names, or the special value ['__SAME_AS_PREVIOUS__']
    if the reference uses em-dashes to indicate same authors as previous entry.
    """
    authors = []

    # Clean up the text - normalize whitespace
    ref_text = re.sub(r'\s+', ' ', ref_text).strip()

    # Check for em-dash pattern meaning "same authors as previous"
    if re.match(r'^[\u2014\u2013\-]{2,}\s*,', ref_text):
        return ['__SAME_AS_PREVIOUS__']

    # Determine where authors section ends based on format

    # IEEE format: authors end at quoted title
    quote_match = re.search(r'["\u201c\u201d]', ref_text)

    # Springer/Nature format: authors end before "(Year)" pattern
    # e.g., "Al Madi N (2023) How Readable..."
    springer_year_match = re.search(r'\s+\((\d{4}[a-z]?)\)\s+', ref_text)

    # ACM format: authors end before ". Year." pattern
    acm_year_match = re.search(r'\.\s*((?:19|20)\d{2})\.\s*', ref_text)

    # USENIX/default: authors end at first "real" period (not after initials like "M." or "J.")
    # Find period followed by space and a word that's not a single capital (another initial)
    first_period = -1
    for match in re.finditer(r'\. ', ref_text):
        pos = match.start()
        # Check what comes before the period - if it's a single capital letter, it's an initial
        if pos > 0:
            char_before = ref_text[pos-1]
            # Check if char before is a single capital (and the char before that is space or start)
            if char_before.isupper() and (pos == 1 or not ref_text[pos-2].isalpha()):
                # This is likely an initial like "M." or "J." - skip it
                continue
        first_period = pos
        break

    # Determine author section based on format detection
    author_end = len(ref_text)

    if quote_match:
        # IEEE format - quoted title
        author_end = quote_match.start()
    elif springer_year_match:
        # Springer/Nature format - "(Year)" after authors
        author_end = springer_year_match.start()
    elif acm_year_match:
        # ACM format - period before year
        author_end = acm_year_match.start() + 1
    elif first_period > 0:
        # USENIX format - first sentence is authors
        author_end = first_period

    author_section = ref_text[:author_end].strip()

    # Remove trailing punctuation
    author_section = re.sub(r'[\.,;:]+$', '', author_section).strip()

    if not author_section:
        return []

    # Check if this is AAAI format (semicolon-separated: "Surname, I.; Surname, I.; and Surname, I.")
    if '; ' in author_section and re.search(r'[A-Z][a-z]+,\s+[A-Z]\.', author_section):
        # AAAI format - split by semicolon
        author_section = re.sub(r';\s+and\s+', '; ', author_section, flags=re.IGNORECASE)
        parts = [p.strip() for p in author_section.split(';') if p.strip()]
        for part in parts:
            # Each part is "Surname, Initials" like "Bail, C. A."
            part = part.strip()
            if part and len(part) > 2 and re.search(r'[A-Z]', part):
                # Convert "Surname, I. M." to a cleaner form for matching
                # Keep as-is since validate_authors normalizes anyway
                authors.append(part)
        return authors[:15]

    # Normalize "and" and "&"
    author_section = re.sub(r',?\s+and\s+', ', ', author_section, flags=re.IGNORECASE)
    author_section = re.sub(r'\s*&\s*', ', ', author_section)

    # Remove "et al."
    author_section = re.sub(r',?\s*et\s+al\.?', '', author_section, flags=re.IGNORECASE)

    # Parse names - split by comma
    parts = [p.strip() for p in author_section.split(',') if p.strip()]

    for part in parts:
        if len(part) < 2:
            continue
        # Skip if it contains numbers (probably not an author)
        if re.search(r'\d', part):
            continue

        # Skip if it has too many words (names are typically 2-4 words)
        words = part.split()
        if len(words) > 5:
            continue

        # Skip if it looks like a sentence/title (has lowercase words that aren't prepositions)
        lowercase_words = [w for w in words if w[0].islower() and w not in ('and', 'de', 'van', 'von', 'la', 'del', 'di')]
        if len(lowercase_words) > 1:
            continue

        # Check if it looks like a name
        if re.search(r'[A-Z]', part) and re.search(r'[a-z]', part):
            name = part.strip()
            if name and len(name) > 2:
                authors.append(name)

    return authors[:15]

def clean_title(title, from_quotes=False):
    """Clean extracted title by removing trailing venue/metadata."""
    if not title:
        return ""

    # Fix hyphenation from PDF line breaks (preserves compound words like "human-centered")
    title = fix_hyphenation(title)

    # If title came from quotes, still apply venue cutoff patterns (quotes may include venue info)
    # but skip the sentence-truncation logic (which doesn't apply to quoted titles)

    # For non-quoted titles, truncate at first sentence-ending period
    # Skip periods that are part of abbreviations (e.g., "U.S." has short segments)
    if not from_quotes:
        for match in re.finditer(r'\.', title):
            pos = match.start()
            # Find start of segment (after last period or space, whichever is later)
            last_period = title.rfind('.', 0, pos)
            last_space = title.rfind(' ', 0, pos)
            segment_start = max(last_period + 1, last_space + 1, 0)
            segment = title[segment_start:pos]
            # If segment > 2 chars, it's likely a real sentence end, not an abbreviation
            # Also treat 2-char ALL-CAPS segments as sentence ends (acronyms like "AI.", "ML.")
            # but not mixed-case abbreviations like "vs.", "al.", "Jr."
            if len(segment) > 2 or (len(segment) == 2 and segment.isupper()):
                # But skip if period is immediately followed by a letter (no space) - product names like "big.LITTLE", "Node.js"
                if pos + 1 < len(title) and title[pos + 1].isalpha():
                    continue
                # Also skip if period is followed by space+digit - version numbers like "Flux. 1", "GPT-4. 0"
                if pos + 2 < len(title) and title[pos + 1] == ' ' and title[pos + 2].isdigit():
                    continue
                title = title[:pos]
                break

    # Also handle "? In" and "? In:" patterns for question-ending titles (Elsevier uses "In:")
    in_venue_match = re.search(r'\?\s*[Ii]n:?\s+(?:[A-Z]|[12]\d{3}\s)', title)
    if in_venue_match:
        title = title[:in_venue_match.start() + 1]  # Keep the question mark

    # Handle "? Journal Name, vol(" pattern (question-ending title leaking into journal)
    q_journal_match = re.search(r'[?!]\s+[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014\-]+,\s*\d+\s*[(:]', title)
    if q_journal_match:
        title = title[:q_journal_match.start() + 1]  # Keep the ?/!

    # Handle "? Automatica 34" or "? IEEE Trans... 53" patterns (journal + volume without comma)
    q_journal_vol_match = re.search(r'[?!]\s+(?:IEEE\s+Trans[a-z.]*|ACM\s+Trans[a-z.]*|Automatica|J\.\s*[A-Z][a-z]+|[A-Z][a-z]+\.?\s+[A-Z][a-z]+\.?)\s+\d+\s*\(', title)
    if q_journal_vol_match:
        title = title[:q_journal_vol_match.start() + 1]  # Keep the ?/!

    # Remove trailing journal/venue info that might have been included
    cutoff_patterns = [
        r'\.\s*[Ii]n:\s+[A-Z].*$',  # Elsevier ". In: Proceedings" or ". In: IFIP"
        r'\.\s*[Ii]n\s+[A-Z].*$',  # Standard ". In Proceedings"
        r'[.?!]\s*(?:Proceedings|Conference|Workshop|Symposium|IEEE|ACM|USENIX|AAAI|EMNLP|NAACL|arXiv|Available|CoRR|PACM[- ]\w+).*$',
        r'[.?!]\s*(?:Advances\s+in|Journal\s+of|Transactions\s+of|Transactions\s+on|Communications\s+of).*$',
        r'[.?!]\s+International\s+Journal\b.*$',  # "? International Journal" or ". International Journal"
        r'\.\s*[A-Z][a-z]+\s+(?:Journal|Review|Transactions|Letters|advances|Processing|medica|Intelligenz)\b.*$',
        r'\.\s*(?:Patterns|Data\s+&\s+Knowledge).*$',
        r'[.,]\s+[A-Z][a-z]+\s+\d+[,\s].*$',  # ". Word Number" or ", Word Number" (journal format like ". Science 344,")
        r',\s*volume\s+\d+.*$',  # ", volume 15"
        r',\s*\d+\s*\(\d+\).*$',  # Volume(issue) pattern
        r',\s*\d+\s*$',  # Trailing volume number
        r'\.\s*\d+\s*$',  # Trailing number after period
        r'\.\s*https?://.*$',  # URLs
        r'\.\s*ht\s*tps?://.*$',  # Broken URLs
        r',\s*(?:vol\.|pp\.|pages).*$',
        r'\.\s*Data\s+in\s+brief.*$',
        r'\.\s*Biochemia\s+medica.*$',
        r'\.\s*KI-Künstliche.*$',
        r'\s+arXiv\s+preprint.*$',  # "arXiv preprint arXiv:..."
        r'\s+arXiv:\d+.*$',  # "arXiv:2503..."
        r'\s+CoRR\s+abs/.*$',  # "CoRR abs/1234.5678"
        r',?\s*(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(?:19|20)\d{2}.*$',  # "June 2024"
        r'[.,]\s*[Aa]ccessed\s+.*$',  # ", Accessed July 23, 2020" (URL access date)
        r'\s*\(\d+[–\-]\d*\)\s*$',  # Trailing page numbers in parens: "(280–28)" or "(280-289)"
        r'\s*\(pp\.?\s*\d+[–\-]\d*\)\s*$',  # "(pp. 280-289)" or "(pp 280–289)"
        r',?\s+\d+[–\-]\d+\s*$',  # Trailing page range: ", 280-289" or " 280–289"
        r',\s+\d{1,4}[–\-]\d{1,4}\s+https?://.*$',  # ", 739–752 https://doi.org/..." (page range + URL)
        r'\.\s*[A-Z][a-zA-Z]+(?:\s+(?:in|of|on|and|for|the|a|an|&|[A-Z]?[a-zA-Z]+))+,\s*\d+\s*[,:]\s*\d+[–\-]?\d*.*$',  # ". Journal Name, vol: pages" like ". Computers in Human Behavior, 61: 280–28"
        r'\.\s*[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]+\d+\s*[(,:]\s*\d+[–\-]?\d*.*$',  # ". Journal Name vol(pages" with extended chars
        r'\.\s*[A-Z][a-zA-Z\s]+[&+]\s*[A-Z].*$',  # ". Words & More" or ". Words + More" (standalone journal names ending with &/+)
        r'\.\s+(?:Beaverton|New\s+York|San\s+Francisco|Cambridge|London|Berlin|Springer|Heidelberg).*$',  # ". Location/Publisher..." (tech report locations)
        r'\.\s+[A-Z][a-z]+\s+of\s+[A-Z][a-z]+(?:\s+(?:and|&)\s+[A-Z][a-z]+)*\s*$',  # ". Journal of Law and Technology" or ". Journal of X"
        r'\.\s+Foundations\s+and\s+Trends.*$',  # ". Foundations and Trends in..."
        r"\.\s+(?:CHI|CSCW|UbiComp|IMWUT|SOUPS|PETS)\s*['\u2019]?\d{2,4}.*$",  # ". CHI'24" or ". CSCW 2024" etc.
        r",\s+(?:CHI|CSCW|UbiComp|IMWUT|SOUPS|PETS)\s*['\u2019]?\d{2,4}.*$",  # ", CHI'24" etc.
    ]

    for pattern in cutoff_patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)

    title = title.strip()
    title = re.sub(r'[.,;:]+$', '', title)

    return title.strip()

MID_SENTENCE_ABBREVIATIONS = {'vs', 'eg', 'ie', 'cf', 'fig', 'figs', 'eq', 'eqs', 'sec', 'ch', 'pt', 'no'}

END_OF_AUTHOR_ABBREVIATIONS = {'al'}

def split_sentences_skip_initials(text):
    """Split text into sentences, but skip periods that are author initials (e.g., 'M.' 'J.') or mid-sentence abbreviations (e.g., 'vs.')."""
    sentences = []
    current_start = 0

    for match in re.finditer(r'\.\s+', text):
        pos = match.start()
        # Check if this period follows a single capital letter (author initial)
        if pos > 0:
            char_before = text[pos-1]
            # If char before is a single capital (and char before that is space/start), it might be an initial
            if char_before.isupper() and (pos == 1 or not text[pos-2].isalpha()):
                # Check what comes AFTER this period to determine if it's really an initial
                # If followed by "Capitalized lowercase" (title pattern), it's a sentence boundary
                # If followed by "Capitalized," or "Capitalized Capitalized," (author pattern), it's an initial
                after_period = text[match.end():]
                # Look at the pattern after the period
                # Author pattern: Capitalized word followed by comma or another capitalized word then comma
                # Surnames can be hyphenated (Aldana-Iuit), have accents (Sánchez), or apostrophes (O'Brien)
                # Also match Elsevier author pattern: "Surname Initial," like "Smith J," or "Smith JK,"
                # Also match "and Surname" pattern for author lists like "J. and Jones, M."
                # Also match another initial "X." or "X.-Y." for IEEE format like "H. W. Chung"
                surname_char = r"[a-zA-Z\u00A0-\u017F''`´\u2018\u2019\-]"  # Letters, accents, apostrophes (including curly quotes U+2018/U+2019), backticks, hyphens
                # Lowercase surname prefixes common in German, Dutch, Spanish, Portuguese, French, Italian names
                surname_prefix = r'(?:von|van|de|del|della|la|le|da|das|dos|der|den|ter|di|du|el|af|ten|op|zum|zur)'
                author_pattern = re.match(rf'^([A-Z]{surname_char}+)\s*,', after_period) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\s+([A-Z][A-Z]?)\s*,', after_period) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\s+[A-Z]{{1,2}},', after_period) or \
                                 re.match(r'^and\s+[A-Z]', after_period, re.IGNORECASE) or \
                                 re.match(r'^[A-Z]\.', after_period) or \
                                 re.match(r'^[A-Z]\.-[A-Z]\.', after_period) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\.\s+[A-Z]', after_period) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\s+and\s+[A-Z]', after_period, re.IGNORECASE) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\s+([A-Z]{surname_char}+)\s*,', after_period) or \
                                 re.match(rf'^{surname_prefix}\s+[A-Z]', after_period, re.IGNORECASE) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\s+([A-Z]{surname_char}+)\.', after_period) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\.\s+\d', after_period) or \
                                 re.match(rf'^([A-Z]{surname_char}+)\.\s+[A-Z][a-z]+\s+[a-z]', after_period) or \
                                 re.match(rf'^[A-Z]\s+([A-Z]{surname_char}+)\s*,', after_period)

                if author_pattern:
                    # This clearly looks like another author - skip this period
                    continue
                # Otherwise (title-like or uncertain pattern), treat as sentence boundary
                # This handles titles starting with proper nouns like "Facebook FAIR's..."

            # Check if this period follows a common abbreviation
            # Find the word before the period
            word_start = pos - 1
            while word_start > 0 and text[word_start-1].isalpha():
                word_start -= 1
            word_before = text[word_start:pos].lower()

            # Mid-sentence abbreviations are never sentence boundaries
            if word_before in MID_SENTENCE_ABBREVIATIONS:
                continue  # Skip this period - it's a mid-sentence abbreviation

            # "et al." is a sentence boundary (ends author list)
            # Don't skip it - let it be treated as a sentence boundary

            # Check if period is followed by a digit (version numbers like "Flux. 1", "GPT-4. 0")
            # These are NOT sentence boundaries - they're part of product/model names
            after_period = text[match.end():]
            if after_period and after_period[0].isdigit():
                continue  # Skip - this is likely a version number

        # This is a real sentence boundary
        sentences.append(text[current_start:pos].strip())
        current_start = match.end()

    # Add the remaining text as the last sentence
    if current_start < len(text):
        sentences.append(text[current_start:].strip())

    return sentences

def extract_title_from_reference(ref_text):
    """Extract title from a reference string.

    Handles three main formats:
    - IEEE: Authors, "Title," in Venue, Year.
    - ACM: Authors. Year. Title. In Venue.
    - USENIX: Authors. Title. In/Journal Venue, Year.

    Returns: (title, from_quotes) tuple where from_quotes indicates if title was in quotes.
    """
    # Fix hyphenation from PDF line breaks (preserves compound words like "human-centered")
    ref_text = fix_hyphenation(ref_text)
    ref_text = re.sub(r'\s+', ' ', ref_text).strip()

    # === General preprocessing ===
    # Strip reference number prefixes like "[1]", "[23]", "1.", "23."
    ref_text = re.sub(r'^\[\d+\]\s*', '', ref_text)
    ref_text = re.sub(r'^\d+\.\s*', '', ref_text)
    # Strip leading punctuation artifacts (sometimes references start with stray period)
    ref_text = ref_text.lstrip('. ')

    # === Math paper preprocessing ===
    # Strip MathReview numbers (e.g., "MR4870047" or "MR 4870047")
    ref_text = re.sub(r'\bMR\s*\d{5,}', '', ref_text)

    # Strip page back-references (e.g., "↑12" or "↑9, 21, 40")
    ref_text = re.sub(r'\s*↑\d+(?:,\s*\d+)*\s*', ' ', ref_text)

    # Clean up any resulting double spaces
    ref_text = re.sub(r'\s+', ' ', ref_text).strip()

    # === Format 1: IEEE/USENIX - Quoted titles or titles with quoted portions ===
    # Handles: "Full Title" or "Quoted part": Subtitle
    # First, try greedy IEEE pattern for titles with nested/inner quotes.
    # Matches from first " to last ," (IEEE convention: title ends with comma inside quotes)
    # e.g. "Autoadmin "what-if" index analysis utility," or "Safe, "Proof-Carrying" AI,"
    greedy_ieee_match = re.search(r'"(.+),"\s', ref_text)
    if greedy_ieee_match:
        title = greedy_ieee_match.group(1).strip()
        # Only accept if reasonably long (short matches may be false positives)
        if len(title.split()) >= 2:
            return title + ',', True

    quote_patterns = [
        r'""([^"]+)""',  # Double double-quotes (escaped quotes in some formats)
        r'["\u201c\u201d]([^"\u201c\u201d]+)["\u201c\u201d]',  # Smart quotes (any combo)
        r'"([^"]+)"',  # Regular quotes
        r'[\u2018]([^\u2018\u2019]{10,})[\u2019]',  # Smart single quotes (Harvard/APA)
        r"(?:^|[\s(])'([^']{10,})'(?:\s*[,.]|\s*$)",  # Plain single quotes with delimiters
    ]

    for pattern in quote_patterns:
        match = re.search(pattern, ref_text)
        if match:
            quoted_part = match.group(1).strip()
            after_quote = ref_text[match.end():].strip()

            # IEEE format: comma inside quotes ("Title,") means title is complete
            # What follows is venue/journal, not a subtitle - skip subtitle detection
            if quoted_part.endswith(','):
                # Quoted titles can be shorter (2 words) - quotes are a strong indicator
                if len(quoted_part.split()) >= 2:
                    return quoted_part, True
                continue  # Try next quote pattern

            # Check if there's a subtitle after the quote
            # Can start with : or - or directly with a capital letter
            # Skip subtitle detection for very short quoted parts (< 2 words) —
            # these are likely inner quotes (e.g. "Proof-Carrying" inside a longer title),
            # not the actual title delimiter.
            if after_quote and len(quoted_part.split()) >= 2:
                # Determine if there's a subtitle and extract it
                subtitle_text = None
                if after_quote[0] in ':-':
                    subtitle_text = after_quote[1:].strip()
                elif after_quote[0].isupper():
                    # Check if it's a venue/journal (not a subtitle)
                    # Common venue starters that should NOT be treated as subtitles
                    venue_starters = r'^(?:IEEE|ACM|USENIX|In\s+|Proc|Trans|Journal|Conference|Workshop|Symposium|vol\.|pp\.)'
                    if not re.match(venue_starters, after_quote, re.IGNORECASE):
                        # Subtitle starts directly with capital letter (no delimiter)
                        subtitle_text = after_quote

                if subtitle_text:
                    # Find where subtitle ends at venue/year markers
                    end_patterns = [
                        r'\.\s*[Ii]n\s+',           # ". In "
                        r'\.\s*(?:Proc|IEEE|ACM|USENIX|NDSS|CCS|AAAI|WWW|CHI|arXiv)',
                        r',\s*[Ii]n\s+',            # ", in "
                        r'\.\s*\((?:19|20)\d{2}\)', # ". (2022)" style venue year
                        r'[,\.]\s*(?:19|20)\d{2}',  # year
                        r'\s+(?:19|20)\d{2}\.',     # year at end
                        r'[.,]\s+[A-Z][a-z]+\s+\d+[,\s]',  # ". Word Number" journal format (". Science 344,")
                        r'\.\s*[A-Z][a-zA-Z]+(?:\s+(?:in|of|on|and|for|the|a|an|&|[A-Za-z]+))+,\s*\d+\s*[,:]',  # ". Journal Name, vol:" like ". Computers in Human Behavior, 61:"
                    ]
                    subtitle_end = len(subtitle_text)
                    for ep in end_patterns:
                        m = re.search(ep, subtitle_text)
                        if m:
                            subtitle_end = min(subtitle_end, m.start())

                    subtitle = subtitle_text[:subtitle_end].strip()
                    subtitle = re.sub(r'[.,;:]+$', '', subtitle)
                    if subtitle and len(subtitle.split()) >= 2:
                        title = f'{quoted_part}: {subtitle}'
                        return title, True

            # No subtitle - just use quoted part if long enough
            if len(quoted_part.split()) >= 3:
                return quoted_part, True

    # === Format 1b: LNCS/Springer - "Authors, I.: Title. In: Venue" ===
    # Pattern: Authors end with initial + colon, then title
    # Example: "Allix, K., Bissyandé, T.F.: Androzoo: Collecting millions. In: Proceedings"
    # Example: "Paulson, L.C.: Extending sledgehammer. Journal of..."
    # Example: "Klein, G., et al.: sel4: Formal verification. In: Proceedings"
    # The colon after author initials marks the start of the title
    # Match: comma/space + Initial(s) + colon (not just any word + colon)
    # Handles: X.: or X.Y.: or X.-Y.: or X.Y.Z.: (multiple consecutive initials)
    # Also handles: "et al.:" pattern
    lncs_match = re.search(r'(?:[,\s][A-Z]\.(?:[-–]?[A-Z]\.)*|et\s+al\.)\s*:\s*(.+)', ref_text)
    if lncs_match:
        after_colon = lncs_match.group(1).strip()
        # Find where title ends - at ". In:" or ". In " or journal patterns or (Year)
        title_end_patterns = [
            r'\.\s*[Ii]n:\s+',           # ". In: " (LNCS uses colon)
            r'\.\s*[Ii]n\s+[A-Z]',       # ". In Proceedings"
            r'\.\s*(?:Proceedings|IEEE|ACM|USENIX|NDSS|arXiv)',
            r'\.\s*(?:Journal|Transactions|Review|Advances)\s+(?:of|on|in)\s+',  # ". Journal of X"
            r'\.\s*[A-Z][a-zA-Z\s]+(?:Access|Journal|Review|Transactions)',  # "X Journal" ending
            r'\.\s*[A-Z][a-z]+\s+\d+\s*\(',  # ". Nature 123(" - journal with volume
            r'\.\s*https?://',           # URL follows title
            r'\.\s*pp?\.\s*\d+',         # Page numbers
            r'\s+\((?:19|20)\d{2}\)\s*[,.]?\s*(?:https?://|$)',  # (Year) followed by URL or end
            r'\s+\((?:19|20)\d{2}\)\s*,',  # (Year) followed by comma
        ]
        title_end = len(after_colon)
        for pattern in title_end_patterns:
            m = re.search(pattern, after_colon)
            if m:
                title_end = min(title_end, m.start())

        title = after_colon[:title_end].strip()
        title = re.sub(r'\.\s*$', '', title)
        # Allow 2-word titles for LNCS format (hyphenated titles count as 1 word)
        # e.g., "Accountable-subgroup multisignatures" is only 2 words
        # Reject if it looks like an author list (ALL CAPS with initials)
        if len(title.split()) >= 2 and not is_likely_author_list(title):
            return title, False

    # === Format 1c: Organization/Documentation - "Organization: Title (Year), URL" ===
    # Pattern: Organization name at START followed by colon, then title
    # Example: "Android Developer: Define custom permissions (2024), https://..."
    # Only match at start of reference to avoid matching mid-title colons
    org_match = re.match(r'^([A-Z][a-zA-Z\s]+):\s*(.+)', ref_text)
    if org_match:
        after_colon = org_match.group(2).strip()
        # Find where title ends - at (Year) followed by URL or comma
        title_end_patterns = [
            r'\s+\((?:19|20)\d{2}\)\s*[,.]?\s*(?:https?://|$)',  # (Year) followed by URL or end
            r'\s+\((?:19|20)\d{2}\)\s*,',  # (Year) followed by comma
            r'\.\s*https?://',           # URL follows title
        ]
        title_end = len(after_colon)
        for pattern in title_end_patterns:
            m = re.search(pattern, after_colon)
            if m:
                title_end = min(title_end, m.start())

        title = after_colon[:title_end].strip()
        title = re.sub(r'\.\s*$', '', title)
        # Allow 2-word titles for this format (documentation titles can be short)
        if len(title.split()) >= 2:
            return title, False

    # === Format 1d: Abbreviated "et al." author - "I. et al. Surname. Title. Venue" ===
    # Pattern: Single initial + "et al." + surname, then title after period
    # Example: "J. et al. Betker. Dall-e 3. https://openai.com/dall-e-3, 2023."
    # Example: "B. et al. Chen. Drct: Diffusion reconstruction contrastive training..."
    # Example: "L. et al. Chai. What makes fake images detectable? In European conference..."
    # This is a non-standard abbreviated format used in some papers
    et_al_match = re.match(r'^[A-Z]\.\s*et\s+al\.\s*([A-Z][a-zA-Z\u00C0-\u024F-]+)\.\s*', ref_text)
    if et_al_match:
        after_author = ref_text[et_al_match.end():]
        # Find where title ends - at venue/URL markers
        title_end_patterns = [
            r'\.\s*[Ii]n\s+[A-Z]',  # ". In Proceedings" / ". In European conference"
            r'\.\s*(?:Proceedings|IEEE|ACM|USENIX|AAAI|CVPR|ICCV|NeurIPS|ICML|arXiv)',
            r'\.\s*[Aa]rXiv\s+preprint',  # ". arXiv preprint"
            r'\.\s*[Aa]dvances\s+in\s+',  # ". Advances in Neural Information"
            r'\.\s*https?://',  # ". https://..."
            r',\s*(?:pages?|pp\.)\s*\d+',  # ", pages 123" or ", pp. 123"
            r',\s*\d+:\d+',  # ", 33:6840" - volume:pages
            r',\s*\d{4}\.$',  # ", 2024." - year at end
        ]
        title_end = len(after_author)
        for pattern in title_end_patterns:
            m = re.search(pattern, after_author)
            if m:
                title_end = min(title_end, m.start())

        if title_end > 0:
            title = after_author[:title_end].strip()
            title = re.sub(r'\.\s*$', '', title)
            # Accept titles with 2+ words (some are short like "Dall-e 3")
            if len(title.split()) >= 2:
                return title, False

    # === Format 2a: Springer/Nature/Harvard - "Authors (Year) Title" or "Authors (Year). Title" ===
    # Pattern: "Surname I, ... (YYYY) Title text. Journal Name Vol(Issue):Pages"
    # Also handles Harvard/APA: "Surname, I. (YYYY). Title. Venue."
    # Year is in parentheses, optionally followed by period, then title
    # IMPORTANT: Reject if year is preceded by ") (" which indicates journal "Vol (Issue) (Year)" format
    # e.g., "IEEE Trans... 25 (7) (2024) 7374" - the (2024) is NOT an author-year pattern
    springer_year_match = re.search(r'\((\d{4}[a-z]?)\)\.?\s+', ref_text)
    if springer_year_match:
        # Check if this looks like a journal "Vol (Issue) (Year)" pattern - reject if so
        before_year = ref_text[:springer_year_match.start()]
        if re.search(r'\)\s*$', before_year):  # Preceded by closing paren = likely "Vol (Issue) (Year)"
            springer_year_match = None
    if springer_year_match:
        after_year = ref_text[springer_year_match.end():]
        # Find where title ends - at journal/venue patterns
        title_end_patterns = [
            r'\.\s*[Ii]n:\s+',  # ". In: " (Springer uses colon)
            r'\.\s*[Ii]n\s+[A-Z]',  # ". In Proceedings"
            r'\.\s*(?:Proceedings|IEEE|ACM|USENIX|arXiv)',
            r'\.\s*[A-Z][a-zA-Z\s]+\d+\s*\(\d+\)',  # ". Journal Name 34(5)" - journal with volume
            r'\.\s*[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014]+\d+:\d+',  # ". Journal Name 34:123" - journal with pages
            r'\.\s*[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]+,\s*\d+',  # ". Journal Name, 11" or ". PACM-HCI, 4"
            r'\.\s*[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]{5,}\s*\((?:19|20)\d{2}\)',  # ". Journal Name (Year)" - Issue #106
            r'[?!]\s+[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]+,\s*\d+\s*[(:]',  # "? Journal Name, vol(" - cut after ?/!
            r'[?!]\s+[A-Z][a-z]+\s+(?:[A-Z][a-z]+\s+)?\d+\(',  # "? Journal 26(" - journal with volume
            r'[?!]\s+[A-Z][a-z]+\s+[a-z]+\s',  # "? Word word " - likely journal after question
            r'\s+\[',  # " [" - editorial note like "[Reprinted...]"
            r'\.\s*https?://',  # URL follows title
            r'\.\s*URL\s+',  # URL follows title
            r'\.\s*Tech\.\s*rep\.',  # Technical report
            r'\.\s*pp?\.\s*\d+',  # Page numbers
        ]
        title_end = len(after_year)
        for pattern in title_end_patterns:
            m = re.search(pattern, after_year)
            if m:
                # For ?/! patterns, keep the punctuation in the title (cut after it)
                if m.group(0)[0] in '?!':
                    title_end = min(title_end, m.start() + 1)
                else:
                    title_end = min(title_end, m.start())

        title = after_year[:title_end].strip()
        title = re.sub(r'\.\s*$', '', title)
        if len(title.split()) >= 3:
            return title, False  # from_quotes=False

    # === Format 2b: ACM - "Authors. Year. Title. In Venue" ===
    # Pattern: ". YYYY. Title-text. In "
    # Use \s+ after year to avoid matching DOIs like "10.1109/CVPR.2022.001234"
    acm_match = re.search(r'\.\s*((?:19|20)\d{2})\.\s+', ref_text)
    if acm_match:
        after_year = ref_text[acm_match.end():]
        # Find where title ends - at ". In " or at venue indicators
        title_end_patterns = [
            r'\.\s*[Ii]n\s+[A-Z]',  # ". In Proceedings"
            r'\.\s*(?:Proceedings|IEEE|ACM|USENIX|arXiv)',
            # ACM short journal format: ". Journal Name Vol (Year), Pages" or ". Journal Name (Year), Pages"
            # Examples: ". Computers & Security 106 (2021), 102277", ". Frontiers in big Data 4 (2021), 729663"
            r'\.\s*[A-Z][a-zA-Z\s&]+\d+\s*\((?:19|20)\d{2}\),\s*\d+',  # ". Journal Vol (Year), Pages"
            r'\.\s*[A-Z][a-zA-Z\s&]+\((?:19|20)\d{2}\),\s*\d+',  # ". Journal (Year), Pages" (no volume)
            r'\.\s*[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]{10,},\s*\d+',  # ". Long Journal Name, vol" - long journal names
            r'\.\s*[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]{5,}\s*\((?:19|20)\d{2}\)',  # ". Journal Name (Year)" - Issue #106
            r'[?!]\s+[A-Z][a-zA-Z\s&+\u00AE\u2013\u2014-]+,\s*\d+\s*[(:]',  # "? Journal Name, vol(" - cut after ?/!
            r'[?!]\s+[A-Z][a-z]+\s+(?:[A-Z][a-z]+\s+)?\d+\(',  # "? Journal 26(" - journal with volume
            r'[?!]\s+[A-Z][a-z]+\s+[a-z]+\s',  # "? Word word " - likely journal after question
            r'\s+doi:',
            r'\.\s*https?://',  # ". https://..." - URL after title (Issue #106)
            r'\s*\(\d+(?:st|nd|rd|th)?\s*ed\.?\)\.\s*[A-Z]',  # "(2nd ed.). Publisher" - book edition + publisher
        ]
        title_end = len(after_year)
        for pattern in title_end_patterns:
            m = re.search(pattern, after_year)
            if m:
                # For ?/! patterns, keep the punctuation in the title (cut after it)
                if m.group(0)[0] in '?!':
                    title_end = min(title_end, m.start() + 1)
                else:
                    title_end = min(title_end, m.start())

        title = after_year[:title_end].strip()
        title = re.sub(r'\.\s*$', '', title)
        if len(title.split()) >= 3:
            return title, False  # from_quotes=False

    # === Format 3: USENIX/ICML/NeurIPS/Elsevier - "Authors. Title. In Venue" or "Authors. Title. In: Venue" ===
    # Find venue markers and extract title before them
    # Order matters: more specific patterns first, generic patterns last
    venue_patterns = [
        r'\.\s*[Ii]n:\s+(?:Proceedings|Workshop|Conference|Symposium|IFIP|IEEE|ACM)',  # Elsevier "In:" format
        r'\.\s*[Ii]n:\s+[A-Z]',  # Elsevier generic "In:" format
        r'\.\s*[Ii]n\s+(?:Proceedings|Workshop|Conference|Symposium|AAAI|IEEE|ACM|USENIX)',
        r'\.\s*[Ii]n\s+[A-Z][a-z]+\s+(?:Conference|Workshop|Symposium)',
        r'\.\s*[Ii]n\s+(?:The\s+)?(?:\w+\s+)+(?:International\s+)?(?:Conference|Workshop|Symposium)',  # ICML/NeurIPS style
        r'\.\s*(?:NeurIPS|ICML|ICLR|CVPR|ICCV|ECCV|AAAI|IJCAI|CoRR|JMLR),',  # Common ML venue abbreviations
        r'\.\s*arXiv\s+preprint',  # arXiv preprint format
        r'\.\s*[Ii]n\s+[A-Z]',  # Generic ". In X" fallback
        r',\s*(?:19|20)\d{2}\.\s*(?:URL|$)',  # Year followed by URL or end - arXiv style (last resort)
        r',\s*(?:19|20)\d{2}\.\s*$',  # Journal format ending with year (last resort)
    ]

    for vp in venue_patterns:
        venue_match = re.search(vp, ref_text)
        if venue_match:
            before_venue = ref_text[:venue_match.start()].strip()

            # First try: Split into sentences using period boundaries
            # This works well for IEEE and many other formats: "Authors. Title. Venue"
            parts = split_sentences_skip_initials(before_venue)
            if len(parts) >= 2:
                title = parts[1].strip()
                title = re.sub(r'\.\s*$', '', title)
                if len(title.split()) >= 3:
                    # Verify it doesn't look like authors (Name Name, pattern)
                    if not re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+,', title):
                        return title, False

            # Second try: For ICML/NeurIPS style where authors and title are in same "sentence"
            # Look for author initial pattern followed by title: "and LastName, I. TitleWords"
            author_end_pattern = r'(?:,\s+[A-Z]\.(?:[-\s]+[A-Z]\.)*|(?:Jr|Sr|III|II|IV)\.)\s+(.)'
            all_matches = list(re.finditer(author_end_pattern, before_venue))

            for match in reversed(all_matches):
                title_start = match.start(1)
                remaining = before_venue[title_start:]

                # Skip if this looks like start of another author: "X.," or "Lastname,"
                if re.match(r'^[A-Z]\.,', remaining) or re.match(r'^[A-Z][a-z]+,', remaining):
                    continue

                title = remaining.strip()
                title = re.sub(r'\.\s*$', '', title)
                if len(title.split()) >= 3:
                    # Verify it doesn't look like authors
                    if not re.match(r'^[A-Z][a-z]+,\s+[A-Z]\.', title):
                        return title, False
                break

            break

    # === Format 4: Journal - "Authors. Title. Journal Name, Vol(Issue), Year" ===
    # Look for journal patterns
    journal_match = re.search(r'\.\s*([A-Z][^.]+(?:Journal|Review|Transactions|Letters|Magazine|Science|Nature|Processing|Advances)[^.]*),\s*(?:vol\.|Volume|\d+\(|\d+,)', ref_text, re.IGNORECASE)
    if journal_match:
        before_journal = ref_text[:journal_match.start()].strip()
        parts = split_sentences_skip_initials(before_journal)
        if len(parts) >= 2:
            title = parts[1].strip()
            if len(title.split()) >= 3:
                return title, False  # from_quotes=False

    # === Format 4b: Elsevier journal - "Authors. Title. Journal Year;Vol(Issue):Pages" ===
    # Example: "Narouei M, Takabi H. Title here. IEEE Trans Dependable Secure Comput 2018;17(3):506–17"
    # Also handles: "Yang L, Chen X. Title here. Secur Commun Netw 2021;2021." (year-only volume)
    # Pattern: Journal name followed by Year;Volume (with optional Issue and Pages)
    elsevier_journal_match = re.search(r'\.\s*([A-Z][A-Za-z\s]+)\s+(?:19|20)\d{2};\d+(?:\(\d+\))?', ref_text)
    if elsevier_journal_match:
        before_journal = ref_text[:elsevier_journal_match.start()].strip()
        parts = split_sentences_skip_initials(before_journal)
        if len(parts) >= 2:
            title = parts[-1].strip()  # Last sentence before journal is likely title
            title = re.sub(r'\.\s*$', '', title)
            if len(title.split()) >= 3:
                return title, False

    # === Format 5: ALL CAPS authors (e.g., "SURNAME, F., AND SURNAME, G. Title here.") ===
    # Only triggers if text starts with a multi-char ALL CAPS surname (not just initials like "H. W.")
    # Skip Chinese ALL CAPS format "SURNAME I, SURNAME I, et al." - handled by Format 8
    # Look for pattern: "SURNAME... [initial]. Title" where Title starts with capital
    if re.match(r'^[A-Z]{2,}', ref_text) and not re.search(r'^[A-Z]{2,}\s+[A-Z](?:,|\s)', ref_text):
        # Find title start: period-space-Capital followed by lowercase word
        # Handles both "A title..." and "Title..." patterns
        title_start_match = re.search(r'\.\s+([A-Z][a-z]*\s+[a-z])', ref_text)
        if title_start_match:
            title_text = ref_text[title_start_match.start(1):]
            # Find title end at venue markers
            title_end_patterns = [
                r'\.\s*[Ii]n\s+[A-Z]',  # ". In Proceedings"
                r'\.\s*(?:Proceedings|IEEE|ACM|USENIX|NDSS|arXiv|Technical\s+report)',
                r'\.\s*[A-Z][a-z]+\s+\d+,\s*\d+\s*\(',  # ". Journal 55, 3 (2012)"
                r'\.\s*(?:Ph\.?D\.?\s+thesis|Master.s\s+thesis)',
            ]
            title_end = len(title_text)
            for pattern in title_end_patterns:
                m = re.search(pattern, title_text)
                if m:
                    title_end = min(title_end, m.start())

            if title_end > 0:
                title = title_text[:title_end].strip()
                title = re.sub(r'\.\s*$', '', title)
                # Reject if it looks like an author list
                if len(title.split()) >= 3 and not is_likely_author_list(title):
                    return title, False

    # === Format 6: Math paper style - "Authors, Title, Venue Vol (Year), Pages" ===
    # Pattern: "Firstname Lastname, ... and Firstname Lastname, Title, Journal Vol (Year)"
    # Title is between the last author comma and the venue comma
    # Example: "Alexander Beilinson, ..., and Ofer Gabber, Faisceaux pervers, Astérisque 100 (1983)"
    # Example: "Aaron Bertram, Moduli of rank-2 vector bundles..., J. Differential Geom. 35 (1992)"
    # Venue patterns: abbreviated journal names followed by volume and (year)
    math_venue_patterns = [
        r',\s*arXiv\s+e-prints\s*\(',  # arXiv e-prints (Month Year)
        r',\s*arXiv:\d+',  # arXiv:XXXX.XXXXX
        r',\s*(?:J\.|Ann\.|Trans\.|Proc\.|Bull\.|Adv\.|Comm\.|Compos\.|Invent\.|Duke|Math\.|Publ\.|Arch\.|Acta|Amer\.|Geom\.|Algebra|Topology)[^,]*\d+\s*\(\d{4}',  # Abbreviated journal + vol (year)
        r',\s*[A-Z][a-zA-Z\u00C0-\u017F\s.\'´`]+\d+\s*\(\d{4}',  # Journal Name Vol (Year) - handles accented chars
        r',\s*IEEE\s+[A-Z][a-zA-Z.\s]+,',  # IEEE Trans/Journal without year in parens
        r',\s*ACM\s+[A-Z][a-zA-Z.\s]+,',  # ACM Trans/Journal without year in parens
        r',\s*Proc\.\s+[A-Z]+',  # Proc. ACM/IEEE etc.
    ]

    for pattern in math_venue_patterns:
        venue_match = re.search(pattern, ref_text)
        if venue_match:
            before_venue = ref_text[:venue_match.start()].strip()

            # Find the title by looking for "and LastName, Title" pattern
            # In math refs, authors end with "and Lastname," then title follows
            # Also handles single author: "Lastname, Title,"
            # Look for the last occurrence of "Name, " that precedes the title
            # The title typically starts with a capital letter and contains multiple words

            # Try to find "and Lastname, Title" pattern first
            # IMPORTANT: "and" must be followed by a proper name, not articles like "and the"
            # Supports name particles: von, van, de, del, di, da, dos, du, le, la, der, den, ten, ter
            and_author_match = re.search(r',?\s+and\s+((?:(?:von|van|de|del|della|di|da|dos|das|du|le|la|les|den|der|ten|ter|op|het)\s+)*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*(.+)', before_venue)
            if and_author_match:
                potential_lastname = and_author_match.group(1).split()[0].lower()
                # Make sure it's not a common word that appears in titles
                common_words = {'the', 'a', 'an', 'some', 'their', 'its', 'other', 'more', 'all', 'new', 'one', 'two'}
                if potential_lastname not in common_words:
                    title = and_author_match.group(2).strip()
                    title = re.sub(r',\s*$', '', title)
                    # Math papers often have shorter titles (e.g., "Faisceaux pervers")
                    # Reject if it looks like an author list
                    if len(title.split()) >= 2 and not is_likely_author_list(title):
                        return title, False

            # Second try: Find where author list ends
            # Authors can be:
            # - "Firstname Lastname" (math style)
            # - "I. Surname" or "I. I. Surname" (CS style with initials)
            # Title starts when we see a comma followed by something that's NOT a name
            # Split by comma and find first non-name segment
            parts = before_venue.split(',')
            title_start_idx = None

            for i, part in enumerate(parts[1:], start=1):  # Skip first part (always author)
                part = part.strip()
                if not part:
                    continue

                # Name particles that appear in multi-part surnames
                _name_particles = {'von', 'van', 'de', 'del', 'della', 'di', 'da', 'dos', 'das',
                                   'du', 'le', 'la', 'les', 'den', 'der', 'ten', 'ter', 'op', 'het', 'do'}

                # Skip "and Firstname Lastname" or "and I. Surname" parts - they're authors
                # Also handles particles: "and de Oliveira Filho"
                if re.match(r'^and\s+(?:(?:von|van|de|del|della|di|da|dos|das|du|le|la|les|den|der|ten|ter|op|het|do)\s+)*(?:[A-Z]\.?\s*)+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$', part):
                    continue

                # Check if this part looks like a name
                # Pattern 1: "Firstname Lastname" - 1-3 capitalized words (may be hyphenated)
                # Pattern 2: "I. Surname" or "I. I. Surname" - initials + surname
                # Pattern 3: "de Oliveira Filho" - particle + surname(s)
                words = part.split()

                # Check for initial-based author: "I. I. Surname" or "I. Surname-Hyphen"
                # Pattern: one or more "X." followed by optional particle + capitalized surname
                if re.match(r'^(?:[A-Z]\.?\s*)+(?:(?:von|van|de|del|della|di|da|dos|das|du|le|la|les|den|der|ten|ter|op|het|do)\s+)*[A-Z][a-z]+(?:-[A-Z][a-z]+)*(?:\s+[A-Z][a-z]+)*$', part):
                    continue  # This is an author with initials

                if len(words) <= 4:
                    # Check if all words look like names, initials, or name particles
                    looks_like_name = all(
                        re.match(r'^[A-Z][a-z]+(?:-[A-Z][a-z]+)*$', w) or  # Capitalized name (hyphenated ok)
                        re.match(r'^[A-Z]\.$', w) or      # Single initial with dot
                        re.match(r'^[A-Z]$', w) or         # Single initial without dot
                        w.lower() in _name_particles        # Name particle (de, van, von, etc.)
                        for w in words
                    )
                    if looks_like_name:
                        continue  # This is part of author list

                # This doesn't look like a name - it's the title start
                title_start_idx = i
                break

            if title_start_idx is not None:
                # Title is from this part to the end
                title = ', '.join(p.strip() for p in parts[title_start_idx:])
                title = re.sub(r',\s*$', '', title)
                # Math papers often have shorter titles
                if len(title.split()) >= 2:
                    return title, False

            break

    # === Format 6b: Elsevier comma-separated - "I. Surname, I. Surname, Title, Venue (Year)" ===
    # Pattern: Authors with single initials (I. Surname) followed by comma-separated title and venue
    # Example: "J. Fan, F. Vercauteren, Somewhat practical fully homomorphic encryption, Cryptology ePrint Archive, Report 2012/144 (2012)."
    # Example: "I. Chillotti, N. Gama, M. Georgieva, M. Izabachène, TFHE: Fast fully homomorphic encryption over the torus, Journal of Cryptology 33 (1) (2020) 34–91."
    # Key: Authors are "I. Surname," pattern, title is comma-separated segment before venue
    # Venue keywords: Journal, Transactions, Archive, Conference, Proceedings, IEEE, ACM, Springer, arXiv
    elsevier_author_pattern = r'^([A-Z]\.(?:\s*[A-Z]\.)*\s+[A-Z][a-zA-Z\u00C0-\u024F-]+(?:\s+[A-Z][a-zA-Z\u00C0-\u024F-]+)*,\s*)+'
    if re.match(elsevier_author_pattern, ref_text):
        # Find venue markers to identify where title ends
        elsevier_venue_patterns = [
            r',\s*(?:Cryptology\s+)?ePrint\s+Archive',  # Cryptology ePrint Archive
            r',\s*arXiv(?:\s+preprint)?(?:\s+arXiv)?[:\s]',  # arXiv preprint
            r',\s*(?:Journal|Transactions|Letters|Review|Annals|Archives?|Bulletin|Communications?|Proceedings?)\s+(?:of\s+)?[A-Z]',  # Journal of X
            r',\s*[A-Z][a-zA-Z\s&]+(?:Journal|Transactions|Letters|Review|Magazine)',  # X Journal, X Transactions
            r',\s*(?:IEEE|ACM|SIAM|AMS|Springer|Elsevier|Wiley)\s+',  # Publisher prefixed venues
            r',\s*(?:in|In):\s+',  # "in:" Elsevier conference format
            r',\s*(?:in|In)\s+(?:Proc\.|Proceedings|Conference|Workshop|Symposium)',  # "in Proceedings"
            r',\s*(?:Proc\.|Proceedings|Conference|Workshop|Symposium)\s+',  # Direct venue start
            r',\s*[A-Z][a-zA-Z.\s]+\d+\s*\(\d+\)\s*\(',  # "Journal Name Vol (Issue) (Year)"
            r',\s*[A-Z][a-zA-Z.\s]+\d+\s*\(\d{4}\)',  # "Journal Name Vol (Year)"
            # Elsevier journal with page numbers: "Journal Vol (Issue) (Year) Pages" or "Journal Vol (Year) Pages"
            # Examples: "IEEE Trans... 25 (7) (2024) 7374–7387", "Future Gen... 141 (2023) 500–513"
            r',\s*[A-Z][a-zA-Z\s&]+\d+\s*\(\d+\)\s*\(\d{4}\)\s*\d+',  # "Journal Vol (Issue) (Year) Pages"
            r',\s*[A-Z][a-zA-Z\s&]+\d+\s*\(\d{4}\)\s*\d+[–-]',  # "Journal Vol (Year) Pages-" (with page range)
            r',\s*(?:Technical\s+)?[Rr]eport\s+',  # Technical report
            r',\s*Ph\.?D\.?\s+[Tt]hesis',  # PhD thesis
            r',\s*[A-Z][a-zA-Z\s]+,\s*(?:vol\.|Vol\.|Volume)\s*\d+',  # "Journal, Vol. X"
        ]

        venue_start = None
        for pattern in elsevier_venue_patterns:
            m = re.search(pattern, ref_text)
            if m:
                if venue_start is None or m.start() < venue_start:
                    venue_start = m.start()

        if venue_start:
            before_venue = ref_text[:venue_start].strip()
            # Split by comma and find where authors end / title begins
            # Authors follow pattern: "I. Surname" or "I. I. Surname"
            parts = before_venue.split(',')
            title_start_idx = None

            for i, part in enumerate(parts):
                part = part.strip()
                if not part:
                    continue

                # Check if this looks like an author: "I. Surname" or "I. I. Surname" or "I.-J. Surname"
                # Also handles: "and I. Surname"
                is_author = bool(re.match(
                    r'^(?:and\s+)?[A-Z]\.(?:\s*[A-Z]\.)*(?:\s*-\s*[A-Z]\.)*\s+[A-Z][a-zA-Z\u00C0-\u024F-]+(?:\s+[A-Z][a-zA-Z\u00C0-\u024F-]+)*$',
                    part
                ))

                if not is_author:
                    # This segment doesn't look like an author - it's the title start
                    title_start_idx = i
                    break

            if title_start_idx is not None and title_start_idx > 0:
                # Title is from this part to the end (before venue)
                title = ', '.join(p.strip() for p in parts[title_start_idx:])
                title = title.strip().rstrip(',')
                if len(title.split()) >= 3:
                    return title, False

    # === Format 7: APA/Harvard - "Surname, I., & Surname, I. (YYYY). Title." ===
    # Pattern: Authors with ampersand, year in parentheses, then title
    # Example: "Dennis, J. E., Jr., & Schnabel, R. B. (1996). Numerical methods..."
    # Example: "Mignemi, G., & Manolopoulou, I. (2025). Bayesian nonparametric..."
    apa_match = re.search(r'&\s+[A-Z][a-z-]+,\s+[A-Z]\..*?\((\d{4})\)\.\s+', ref_text)
    if apa_match:
        after_year = ref_text[apa_match.end():]
        # Title ends at period followed by journal name or URL
        title_end_patterns = [
            r'\.\s+[A-Z][a-z]+(?:\s+[A-Z]?[a-z]+)*,?\s+\d+',  # ". Journal Name, vol" or ". Journal Name 26"
            r'\.\s+[Ii]n\s+',  # ". In "
            r'\.\s+(?:http|doi:|arXiv)',  # ". URL/DOI"
            r'\.\s+[A-Z][a-z]+:',  # ". Publisher:"
            r'\s+\[',  # " [" - editorial note like "[Originally published...]"
            r'\.\s*$',  # End of string
        ]
        title_end = len(after_year)
        for pattern in title_end_patterns:
            m = re.search(pattern, after_year)
            if m:
                title_end = min(title_end, m.start())

        title = after_year[:title_end].strip()
        title = re.sub(r'\.\s*$', '', title)
        if len(title.split()) >= 3:
            return title, False

    # === Format 8: ALL CAPS Chinese/Biomedical - "SURNAME I, SURNAME I, et al. Title" ===
    # Pattern: All caps surnames with single-letter initials (Chinese biomedical style)
    # Example: "CAO X, YANG B, WANG K, et al. Title of the paper. Journal 2024"
    # Example: "LIU Z, SABERI A, et al. H∞ almost state synchronization..."
    # Authors are ALL CAPS, followed by "et al." or sentence end, then title
    all_caps_match = re.search(r'^([A-Z]{2,})\s+[A-Z](?:,|\s|$)', ref_text)
    if all_caps_match:
        # Find end of author list: "et al." or transition to non-caps content
        et_al_match = re.search(r',?\s+et\s+al\.?\s*[,.]?\s*', ref_text, re.IGNORECASE)
        if et_al_match:
            after_authors = ref_text[et_al_match.end():].strip()
        else:
            # Find where ALL CAPS author pattern ends
            # Pattern: "SURNAME X, SURNAME Y, Title starts here"
            # Title typically starts with a sentence that has mixed case
            parts = ref_text.split(', ')
            title_start_idx = None
            for i, part in enumerate(parts):
                part = part.strip()
                # Check if this looks like an ALL CAPS author (SURNAME X or just SURNAME)
                if re.match(r'^[A-Z]{2,}(?:\s+[A-Z])?$', part):
                    continue  # Still in author list
                # Found non-author part - this is the title start
                title_start_idx = i
                break

            if title_start_idx is not None:
                after_authors = ', '.join(parts[title_start_idx:]).strip()
            else:
                after_authors = None

        if after_authors:
            # Find where title ends - at journal/year markers
            title_end_patterns = [
                r'\[J\]',  # Chinese citation marker for journal
                r'\[C\]',  # Chinese citation marker for conference
                r'\[M\]',  # Chinese citation marker for book
                r'\[D\]',  # Chinese citation marker for dissertation
                r'\.\s*[A-Z][a-zA-Z\s]+\d+\s*\(\d+\)',  # ". Journal Name 34(5)"
                r'\.\s*[A-Z][a-zA-Z\s&+]+\d+:\d+',  # ". Journal 34:123"
                r'\.\s*[A-Z][a-zA-Z\s&+]+,\s*\d+',  # ". Journal Name, vol"
                r'\.\s*(?:19|20)\d{2}',  # ". 2024"
                r'\.\s*https?://',
                r'\.\s*doi:',
            ]
            title_end = len(after_authors)
            for pattern in title_end_patterns:
                m = re.search(pattern, after_authors)
                if m:
                    title_end = min(title_end, m.start())

            title = after_authors[:title_end].strip()
            title = re.sub(r'\.\s*$', '', title)
            # Reject if it looks like an author list
            if len(title.split()) >= 3 and not is_likely_author_list(title):
                return title, False

    # === Fallback: second sentence if it looks like a title ===
    # Use smart splitting that skips author initials like "M." "J."
    sentences = split_sentences_skip_initials(ref_text)
    if len(sentences) >= 2:
        # First sentence is likely authors, second might be title
        potential_title = sentences[1].strip()

        # Skip if it looks like authors
        words = potential_title.split()
        if words:
            # Count name-like patterns (Capitalized words)
            cap_words = sum(1 for w in words if re.match(r'^[A-Z][a-z]+$', w))
            # Count "and" conjunctions
            and_count = sum(1 for w in words if w.lower() == 'and')

            # If high ratio of cap words and "and", probably authors
            if len(words) > 0 and (cap_words / len(words) > 0.7) and and_count > 0:
                # Try third sentence
                if len(sentences) >= 3:
                    potential_title = sentences[2].strip()

        # Skip if starts with "In " (venue) or looks like an author list
        if not re.match(r'^[Ii]n\s+', potential_title) and not is_likely_author_list(potential_title):
            if len(potential_title.split()) >= 3:
                return potential_title, False  # from_quotes=False

    return "", False

def extract_venue_from_reference(ref_text: str, title: str) -> str:
    """Extract a venue/journal string from the citation tail after the title."""
    if not title:
        return ""

    compact_ref = re.sub(r"\s+", " ", ref_text).strip()
    title_candidates = [
        title.strip(),
        title.strip().strip('"\u201c\u201d\''),
        title.strip().rstrip(".,;:!?"),
        title.strip().strip('"\u201c\u201d\'').rstrip(".,;:!?"),
    ]

    tail = ""
    seen_candidates = set()
    for candidate in title_candidates:
        candidate = candidate.strip()
        if not candidate or candidate in seen_candidates:
            continue
        seen_candidates.add(candidate)
        match = re.search(re.escape(candidate) + r"(?P<tail>.*)$", compact_ref, re.IGNORECASE)
        if match:
            tail = match.group("tail")
            break

    if not tail:
        return ""

    venue = tail.strip()
    venue = re.sub(r'^[\s"\u201c\u201d\'`,.;:)\]-]+', "", venue)
    venue = re.sub(r"^(?:[.\-]\s*)?[Ii]n:?\s+", "", venue)
    venue = re.sub(r"^\(\s*(?:19|20)\d{2}[a-z]?\s*\)\.?\s*", "", venue)
    venue = re.sub(r"^(?:19|20)\d{2}[a-z]?\.\s*", "", venue)
    venue = re.sub(r"\s*(?:doi\s*:|https?://doi\.org/)\S+.*$", "", venue, flags=re.IGNORECASE)
    venue = re.sub(r"\s*https?://\S+.*$", "", venue, flags=re.IGNORECASE)

    cut_patterns = [
        r",\s*vol\.\s*\d.*$",
        r",\s*no\.\s*\d.*$",
        r",\s*issue\s+\d.*$",
        r",\s*pp?\.\s*.*$",
        r"\.\s*pp?\.\s*.*$",
        r",\s*pages?\s*.*$",
        r",\s*\d{1,4}\s*[-\u2013]\s*\d{1,4}.*$",
        r"\s*\(\s*(?:19|20)\d{2}[a-z]?\s*\).*$",
        r",\s*(?:19|20)\d{2}[a-z]?\.*$",
        r"\.\s*(?:19|20)\d{2}[a-z]?\.*$",
    ]
    for pattern in cut_patterns:
        venue = re.sub(pattern, "", venue, flags=re.IGNORECASE)

    venue = venue.strip(" ,.;:")
    venue = re.sub(r"\s+", " ", venue)

    if not venue:
        return ""
    normalized_venue = re.sub(r"[^a-z0-9]+", "", venue.lower())
    normalized_title = re.sub(r"[^a-z0-9]+", "", title.lower())
    if normalized_venue == normalized_title:
        return ""
    if is_non_reference_content(venue):
        return ""
    return venue

def parse_references_from_text(pdf_text: str, source_pdf: str) -> list[dict[str, str]]:
    """Parse references from extracted PDF text using local hallucinator-derived logic."""
    ref_section = find_references_section(pdf_text)
    if not ref_section:
        raise RuntimeError("Could not locate references section")

    raw_refs = segment_references(ref_section)
    rows: list[dict[str, str]] = []
    previous_authors: list[str] = []

    for ref_index, ref_text in enumerate(raw_refs, start=1):
        doi = extract_doi(ref_text) or ""
        arxiv_id = extract_arxiv_id(ref_text) or ""

        ref_text = re.sub(r"\n\d{1,4}\n", "\n", ref_text)
        ref_text = fix_hyphenation(ref_text)

        if re.search(r"https?\s*:\s*//", ref_text) or re.search(r"ht\s*tps?\s*:\s*//", ref_text):
            trusted_domain = re.search(
                r"(acm\.org|ieee\.org|usenix\.org|arxiv\.org|doi\.org)",
                ref_text,
                re.IGNORECASE,
            )
            has_descriptive_title = re.search(r'[“"][^”"]{8,}[”"]', ref_text)
            has_year = re.search(r"\b(?:19|20)\d{2}\b", ref_text)
            # Keep web references when they look like real citations with a title and year,
            # and only skip likely URL-only/non-scholarly entries.
            if not trusted_domain and not (has_descriptive_title and has_year):
                continue

        title, from_quotes = extract_title_from_reference(ref_text)
        title = truncate_title_at_venue(title)
        title = clean_title(title, from_quotes=from_quotes)

        if not title or len(title.split()) < 4:
            continue
        if is_venue_only(title):
            continue
        if is_non_reference_content(title):
            continue

        venue = extract_venue_from_reference(ref_text, title)

        authors = extract_authors_from_reference(ref_text)
        if authors == ["__SAME_AS_PREVIOUS__"]:
            authors = previous_authors if previous_authors else []

        if authors:
            previous_authors = authors

        raw_citation = re.sub(r"\s+", " ", ref_text).strip()
        raw_citation = re.sub(r"^\[\d+\]\s*", "", raw_citation)
        raw_citation = re.sub(r"^\d+\.\s*", "", raw_citation)

        rows.append({
            "source_pdf": source_pdf,
            "reference_id": str(ref_index),
            "title": title,
            "authors": "; ".join(authors),
            "venue": venue,
            "doi": doi,
            "arxiv_id": arxiv_id,
            "raw_citation": raw_citation,
        })

    return rows
