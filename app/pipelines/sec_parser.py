"""SEC Filing Parser - Extracts structured blocks from HTML/XBRL filings.
Supports: 10-K, 10-Q, 8-K with tailored extraction logic per form type.
"""
import re
import logging
from dataclasses import dataclass
from typing import Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# XBRL metadata noise patterns 
XBRL_NOISE_PATTERNS = [
    r'Namespace Prefix:',
    r'Data Type:\s*xbrli:',
    r'Balance Type:\s*(?:na|credit|debit)',
    r'Period Type:\s*(?:duration|instant)',
    r'\bdei_\w+\b',
    r'\bus-gaap_\w+\b',
    r'xbrli:\w+ItemType',
    r'Definition.*References',
    r'Reference 1:\s*http://www\.xbrl\.org',
    r'-Publisher SEC\s*-Name',
    r'No definition available\.',
]
XBRL_NOISE_RE = re.compile('|'.join(XBRL_NOISE_PATTERNS), re.IGNORECASE)

# Inline XBRL cruft to strip from text (units, prefixes)
XBRL_CRUFT_RE = re.compile(r'\b(?:iso4217:\w+|xbrli:\w+)\b', re.IGNORECASE)

# Reference patterns to strip entirely
XBRL_REFERENCE_RE = re.compile(
    r'Reference \d+:\s*http://www\.xbrl\.org/\d+/role/\w+\s*'
    r'(?:-Publisher\s+\w+\s*)?(?:-Name\s+[\w\s]+)?(?:-Number\s+\d+\s*)?'
    r'(?:-Section\s+[\w\-]+\s*)?(?:-Subsection\s+[\w\-]+\s*)?',
    re.IGNORECASE
)

# Form-specific section patterns
FORM_SECTION_PATTERNS = {
    "10-K": [
        (r'^(?:ITEM|Item)\s*(\d{1,2}[A-Da-d]?)\.?\s*[-–—.]?\s*(.*)$', 'item'),
        (r'^(?:PART|Part)\s+([IVX]+)\.?\s*[-–—.]?\s*(.*)$', 'part'),
    ],
    "10-Q": [
        (r'^(?:ITEM|Item)\s*(\d{1,2}[A-Da-d]?)\.?\s*[-–—.]?\s*(.*)$', 'item'),
        (r'^(?:PART|Part)\s+([IVX]+)\.?\s*[-–—.]?\s*(.*)$', 'part'),
    ],
    "8-K": [
        (r'^(?:ITEM|Item)\s*(\d+\.?\d*)\.?\s*[-–—.]?\s*(.*)$', 'item'),
        (r'^(?:SECTION|Section)\s*(\d+)\.?\s*[-–—.]?\s*(.*)$', 'section'),
        (r'^(?:Forward.Looking\s+Statements?)(.*)$', 'forward_looking'),
    ],
}

# 8-K specific item titles (most common)
ITEM_TITLES_8K = {
    "1.01": "Entry into Material Definitive Agreement",
    "1.02": "Termination of Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of Direct Financial Obligation",
    "2.04": "Triggering Events That Accelerate Obligations",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting",
    "3.02": "Unregistered Sales of Equity Securities",
    "3.03": "Material Modification to Rights",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure/Election of Directors or Officers",
    "5.03": "Amendments to Articles or Bylaws",
    "5.04": "Temporary Suspension of Trading",
    "5.05": "Amendment to Registrant's Code of Ethics",
    "5.06": "Change in Shell Company Status",
    "5.07": "Submission of Matters to Vote of Security Holders",
    "5.08": "Shareholder Nominations",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}

# 10-Q item titles by Part (Part I = Financial, Part II = Other)
ITEM_TITLES_10Q = {
    "I": {
        "1": "Financial Statements",
        "2": "Management's Discussion and Analysis",
        "3": "Quantitative and Qualitative Disclosures About Market Risk",
        "4": "Controls and Procedures",
    },
    "II": {
        "1": "Legal Proceedings",
        "1A": "Risk Factors",
        "2": "Unregistered Sales of Equity Securities",
        "3": "Defaults Upon Senior Securities",
        "4": "Mine Safety Disclosures",
        "5": "Other Information",
        "6": "Exhibits",
    },
}


@dataclass
class Block:
    """A parsed content block from an SEC filing."""
    text: str
    type: str      # 'text' | 'table' | 'header' | 'item_header' | 'exhibit'
    page: int
    is_table: bool
    section_hint: Optional[str] = None  # For form-specific section detection


class SECParser:
    """Parses SEC HTML/XBRL filings into structured blocks.
    
    Supports form-specific parsing:
    - 10-K: Annual report with standard Items 1-16
    - 10-Q: Quarterly report with Part I/II Items
    - 8-K: Current report with event-based Items (1.01, 2.02, etc.)
    """
    
    STRIP_PATTERNS = [
        (r'<SEC-DOCUMENT>.*?<DOCUMENT>', ''),
        (r'<TYPE>.*?<TEXT>', ''),
        (r'</?ix:[^>]+>', ''),
        (r'</?dei:[^>]+>', ''),
        (r'</?us-gaap:[^>]+>', ''),
    ]
    
    def __init__(self, min_text_len: int = 10, max_table_rows: int = 25, form_type: str = "10-K"):
        self.min_text_len = min_text_len
        self.max_table_rows = max_table_rows
        self.form_type = form_type.upper()
        self.stats = {'blocks': 0, 'tables': 0, 'xbrl_noise_skipped': 0, 'duplicates_removed': 0, 'pages': 1}
        self._seen_sentences = set()
        self._current_10q_part = "I"  # Track Part I/II for 10-Q
        self._section_patterns = self._compile_section_patterns()
    
    def _compile_section_patterns(self) -> list:
        """Compile regex patterns for current form type."""
        patterns = FORM_SECTION_PATTERNS.get(self.form_type, FORM_SECTION_PATTERNS["10-K"])
        return [(re.compile(p, re.IGNORECASE), t) for p, t in patterns]
    
    def set_form_type(self, form_type: str):
        """Update form type and recompile patterns."""
        self.form_type = form_type.upper()
        self._current_10q_part = "I"  # Reset part tracking
        self._section_patterns = self._compile_section_patterns()
    
    def _detect_section_header(self, text: str) -> Optional[tuple]:
        """Detect if text is a section header for current form type.
        Returns (section_id, section_title, header_type) or None.
        """
        for pattern, header_type in self._section_patterns:
            match = pattern.match(text.strip())
            if match:
                section_num = match.group(1).upper() if match.lastindex >= 1 else ""
                section_title = match.group(2).strip() if match.lastindex >= 2 else ""
                
                # Track Part transitions for 10-Q
                if self.form_type == "10-Q" and header_type == "part":
                    self._current_10q_part = section_num  # "I" or "II"
                
                # Get standard title based on form type
                if len(section_title) < 5:
                    if self.form_type == "8-K" and section_num in ITEM_TITLES_8K:
                        section_title = ITEM_TITLES_8K[section_num]
                    elif self.form_type == "10-Q" and header_type == "item":
                        part_titles = ITEM_TITLES_10Q.get(self._current_10q_part, {})
                        section_title = part_titles.get(section_num, "")
                
                return (section_num, section_title, header_type)
        return None
    
    def parse(self, content: str, form_type: str = None) -> list[Block]:
        """Parse filing content and return list of content blocks.
        
        Args:
            content: Raw HTML/XBRL filing content
            form_type: Override form type (10-K, 10-Q, 8-K)
        """
        if form_type:
            self.set_form_type(form_type)
        
        self.stats = {'blocks': 0, 'tables': 0, 'xbrl_noise_skipped': 0, 'duplicates_removed': 0, 'pages': 1}
        self._seen_sentences = set()
        self._current_10q_part = "I"  # Reset for each parse
        html = content
        
        logger.debug("Parsing %s filing, content length: %d chars", self.form_type, len(content))
        
        for pattern, repl in self.STRIP_PATTERNS:
            html = re.sub(pattern, repl, html, flags=re.DOTALL)
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove XBRL reference/definition metadata tables (authRefData class)
        for tag in soup.find_all(class_='authRefData'):
            tag.decompose()
        
        # Remove hidden elements (display: none) - these often contain XBRL metadata
        for tag in soup.find_all(style=re.compile(r'display:\s*none', re.I)):
            tag.decompose()
        
        # Remove JSON-LD and other script/style elements
        for tag in soup(['script', 'style', 'meta', 'link', 'head']):
            tag.decompose()
        
        # First pass: process and remove tables to prevent duplicate content in parent elements
        processed_tables = {}
        for table in soup.find_all('table'):
            md = self._table_to_md(table)
            if md and not self._is_xbrl_noise(md):
                # Store the markdown with a placeholder
                table_id = id(table)
                processed_tables[table_id] = md
            table.decompose()  # Remove from DOM so parent get_text() won't include it
        
        blocks, page = [], 1
        table_idx = 0
        table_list = list(processed_tables.values())
        
        for el in soup.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'hr']):
            if el.name == 'hr' or 'page-break' in el.get('style', ''):
                page += 1
                continue
            text = el.get_text(' ', strip=True)
            # Clean inline XBRL cruft (iso4217:USD, xbrli:shares, etc.)
            text = XBRL_CRUFT_RE.sub('', text).strip()
            # Strip XBRL reference patterns
            text = XBRL_REFERENCE_RE.sub('', text).strip()
            text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
            
            # Deduplicate at sentence level
            text = self._deduplicate_text(text)
            
            if len(text) > self.min_text_len:
                if self._is_xbrl_noise(text):
                    self.stats['xbrl_noise_skipped'] += 1
                    logger.debug("Skipped XBRL metadata text at page %d", page)
                    continue
                
                # Detect section headers for form-specific parsing
                section_info = self._detect_section_header(text)
                if section_info:
                    section_id, section_title, header_type = section_info
                    hint = f"{header_type}:{section_id}"
                    blocks.append(Block(text, 'item_header', page, False, section_hint=hint))
                else:
                    btype = 'header' if el.name in ['h1','h2','h3','h4'] else 'text'
                    blocks.append(Block(text, btype, page, False))
        
        # Now add all the pre-processed tables (order approximated by page tracking)
        # Insert tables interspersed based on document flow
        for md in table_list:
            blocks.append(Block(md, 'table', page, True, section_hint=None))
            self.stats['tables'] += 1
        
        self.stats['blocks'] = len(blocks)
        self.stats['pages'] = page
        
        logger.info("Parsed %d blocks (%d tables), skipped %d XBRL noise, removed %d duplicates, %d pages",
                   self.stats['blocks'], self.stats['tables'], 
                   self.stats['xbrl_noise_skipped'], self.stats['duplicates_removed'], self.stats['pages'])
        
        return blocks
    
    def _deduplicate_text(self, text: str) -> str:
        """Remove duplicate sentences within text and across blocks."""
        # First, remove consecutive duplicate phrases (e.g., "UNITED STATES UNITED STATES")
        text = self._remove_consecutive_duplicates(text)
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        unique_sentences = []
        
        for sentence in sentences:
            # Normalize for comparison (lowercase, strip extra whitespace)
            normalized = ' '.join(sentence.lower().split())
            if len(normalized) > 20 and normalized in self._seen_sentences:
                self.stats['duplicates_removed'] += 1
                continue
            if len(normalized) > 20:
                self._seen_sentences.add(normalized)
            unique_sentences.append(sentence)
        
        return ' '.join(unique_sentences)
    
    def _remove_consecutive_duplicates(self, text: str) -> str:
        """Remove consecutive duplicate phrases like 'WORD WORD' or 'phrase here phrase here'."""
        words = text.split()
        if len(words) < 4:
            return text
        
        # Check for consecutive duplicate n-grams (2-8 words)
        for n in range(8, 1, -1):  # Start with larger phrases
            i = 0
            result = []
            while i < len(words):
                # Check if next n words match the following n words
                if i + 2 * n <= len(words):
                    phrase1 = ' '.join(words[i:i+n])
                    phrase2 = ' '.join(words[i+n:i+2*n])
                    if phrase1.lower() == phrase2.lower() and len(phrase1) > 10:
                        result.extend(words[i:i+n])
                        i += 2 * n  # Skip the duplicate
                        self.stats['duplicates_removed'] += 1
                        continue
                result.append(words[i])
                i += 1
            words = result
        
        return ' '.join(words)
    
    def _is_xbrl_noise(self, text: str) -> bool:
        """Check if content is XBRL structural metadata."""
        matches = len(XBRL_NOISE_RE.findall(text))
        return matches >= 2
    
    def _table_to_md(self, table) -> str:
        """Convert HTML table to markdown format."""
        rows = table.find_all('tr')[:self.max_table_rows]
        if not rows:
            return ""
        
        md = []
        for i, row in enumerate(rows):
            cells = [c.get_text(' ', strip=True)[:50] for c in row.find_all(['td', 'th'])]
            if cells:
                md.append('| ' + ' | '.join(cells) + ' |')
                if i == 0:
                    md.append('|' + '---|' * len(cells))
        
        return '\n'.join(md) if len(md) > 2 else ""
