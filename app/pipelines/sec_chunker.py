import re
import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from .sec_parser import SECParser, Block, ITEM_TITLES_8K, ITEM_TITLES_10Q

logger = logging.getLogger(__name__)

# Comprehensive ITEM patterns for all SEC form types
ITEM_PATTERNS = {
    "10-K": r'^(?:ITEM|Item)\s*(\d{1,2}[A-Da-d]?)\.?\s*[-–—.]?\s*(.*)$',
    "10-Q": r'^(?:ITEM|Item)\s*(\d{1,2}[A-Da-d]?)\.?\s*[-–—.]?\s*(.*)$',
    "8-K":  r'^(?:ITEM|Item)\s*(\d+\.?\d*)\.?\s*[-–—.]?\s*(.*)$',
}

# Standard 10-K Item titles 
ITEM_TITLES_10K = {
    "1": "Business",
    "1A": "Risk Factors",
    "1B": "Unresolved Staff Comments",
    "1C": "Cybersecurity",
    "1D": "Information about Executive Officers",
    "2": "Properties",
    "3": "Legal Proceedings",
    "4": "Mine Safety Disclosures",
    "5": "Market for Registrant's Common Equity",
    "6": "Reserved",
    "7": "Management's Discussion and Analysis",
    "7A": "Quantitative and Qualitative Disclosures About Market Risk",
    "8": "Financial Statements and Supplementary Data",
    "9": "Changes in and Disagreements with Accountants",
    "9A": "Controls and Procedures",
    "9B": "Other Information",
    "9C": "Disclosure Regarding Foreign Jurisdictions",
    "10": "Directors, Executive Officers and Corporate Governance",
    "11": "Executive Compensation",
    "12": "Security Ownership",
    "13": "Certain Relationships and Related Transactions",
    "14": "Principal Accountant Fees and Services",
    "15": "Exhibits and Financial Statement Schedules",
    "16": "Form 10-K Summary",
}

# Form-specific chunking configs
FORM_CONFIGS = {
    "10-K": {
        "target_chunk_size": 400,
        "min_chunk_size": 100,
        "max_chunk_size": 800,
        "overlap_size": 50,
        "item_titles": ITEM_TITLES_10K,
    },
    "10-Q": {
        "target_chunk_size": 350,
        "min_chunk_size": 80,
        "max_chunk_size": 700,
        "overlap_size": 40,
        "item_titles": ITEM_TITLES_10Q,
    },
    "8-K": {
        "target_chunk_size": 300,  # 8-Ks are shorter, smaller chunks
        "min_chunk_size": 50,
        "max_chunk_size": 600,
        "overlap_size": 30,
        "item_titles": ITEM_TITLES_8K,
    },
}

# Boilerplate patterns to filter out
BOILERPLATE_PATTERNS = [
    r'^table\s*of\s*contents?$',
    r'^signatures?$',
    r'^page\s*\d+$',
    r'^\d+$',  # Just page numbers
    r'^part\s+[ivx]+$',
    r'^exhibit\s+index$',
    r'^None\.?$',
    r'^N/?A\.?$',
]
BOILERPLATE_RE = re.compile('|'.join(BOILERPLATE_PATTERNS), re.IGNORECASE)


@dataclass
class Chunk:
    """A RAG-ready chunk with metadata."""
    id: str
    content: str
    ticker: str
    form_type: str
    year: str
    accession_number: str
    section_id: str
    section_title: str
    page: int
    has_table: bool
    word_count: int
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Section:
    """A document section containing blocks."""
    id: str
    title: str
    blocks: list
    start_page: int


class SECChunker:
    
    # Pattern to extract core data from table rows for dedup comparison
    TABLE_DATA_RE = re.compile(r'[\|\-\s]+')
    
    def _get_item_title(self, item_num: str) -> str:
        """Get the standard title for an item number.
        Handles 10-Q nested Part I/II structure.
        """
        if self.form_type == "10-Q":
            # Try current part first, then fallback to other part
            title = self.item_titles.get(self._current_10q_part, {}).get(item_num, "")
            if not title:
                # Try the other part
                other_part = "II" if self._current_10q_part == "I" else "I"
                title = self.item_titles.get(other_part, {}).get(item_num, "")
            return title
        else:
            return self.item_titles.get(item_num, "")
    
    def __init__(
        self, 
        form_type: str = "10-K", 
        target_chunk_size: int = None,
        min_chunk_size: int = None,
        max_chunk_size: int = None,
        overlap_size: int = None,
        min_table_words: int = 75
    ):
        self.form_type = form_type.upper()
        self._current_10q_part = "I"  # Track Part I/II for 10-Q
        self._apply_form_config(target_chunk_size, min_chunk_size, max_chunk_size, overlap_size)
        self.min_table_words = min_table_words
        self.pattern = re.compile(ITEM_PATTERNS.get(self.form_type, ITEM_PATTERNS["10-K"]))
        self._seen_content = set()
        self.stats = {
            'sections': 0, 
            'chunks': 0, 
            'tables': 0, 
            'trivial_skipped': 0,
            'boilerplate_skipped': 0,
            'merged_undersized': 0,
            'content_deduped': 0
        }
    
    def _apply_form_config(self, target=None, min_size=None, max_size=None, overlap=None):
        """Apply form-specific chunking configuration."""
        config = FORM_CONFIGS.get(self.form_type, FORM_CONFIGS["10-K"])
        self.target_chunk_size = target or config["target_chunk_size"]
        self.min_chunk_size = min_size or config["min_chunk_size"]
        self.max_chunk_size = max_size or config["max_chunk_size"]
        self.overlap_size = overlap or config["overlap_size"]
        self.item_titles = config.get("item_titles", ITEM_TITLES_10K)
        logger.debug("Applied %s config: target=%d, min=%d, max=%d, overlap=%d",
                    self.form_type, self.target_chunk_size, self.min_chunk_size,
                    self.max_chunk_size, self.overlap_size)
    
    def process(self, blocks: list[Block], form_type: str = None, accession: str = None, 
                ticker: str = "UNKNOWN", year: str = "2025") -> list[Chunk]:
        """Chunk parsed blocks into RAG-ready segments."""
        self._seen_content = set()
        self._current_10q_part = "I"  # Reset for each process
        self.stats = {
            'sections': 0, 
            'chunks': 0, 
            'tables': 0, 
            'trivial_skipped': 0,
            'boilerplate_skipped': 0,
            'merged_undersized': 0,
            'content_deduped': 0
        }
        
        if form_type:
            self.form_type = form_type.upper()
            self._apply_form_config()
            self.pattern = re.compile(ITEM_PATTERNS.get(self.form_type, ITEM_PATTERNS["10-K"]))
        
        logger.debug("Processing %d blocks for %s %s (%s)", len(blocks), ticker, form_type, self.form_type)
        
        # Filter boilerplate blocks first
        filtered_blocks = self._filter_boilerplate(blocks)
        logger.debug("After boilerplate filter: %d blocks (filtered %d)", 
                    len(filtered_blocks), len(blocks) - len(filtered_blocks))
        
        # Extract sections based on ITEM headers
        sections = self._extract_sections(filtered_blocks)
        self.stats['sections'] = len(sections)
        
        chunks = []
        for section in sections:
            section_chunks = self._chunk_section(section, ticker, year, accession)
            chunks.extend(section_chunks)
            logger.debug("Section '%s': %d chunks", section.id, len(section_chunks))
        
        # Merge undersized chunks
        chunks = self._merge_undersized_chunks(chunks)
        
        self.stats['chunks'] = len(chunks)
        self.stats['tables'] = sum(1 for c in chunks if c.has_table)
        
        logger.info(
            "Chunked into %d chunks (%d tables) across %d sections | "
            "skipped: %d trivial, %d boilerplate, %d deduped | merged: %d undersized",
            self.stats['chunks'], self.stats['tables'], self.stats['sections'],
            self.stats['trivial_skipped'], self.stats['boilerplate_skipped'],
            self.stats['content_deduped'], self.stats['merged_undersized']
        )
        
        return chunks
    
    def _get_item_title(self, item_num: str) -> str:
        """Get standard title for item number."""
        if self.form_type == "10-Q":
            title = self.item_titles.get(self._current_10q_part, {}).get(item_num, "")
            if not title:
                other_part = "II" if self._current_10q_part == "I" else "I"
                title = self.item_titles.get(other_part, {}).get(item_num, "")
            return title
        else:
            return self.item_titles.get(item_num, "")
    
    def _filter_boilerplate(self, blocks: list[Block]) -> list[Block]:
        """Filter out boilerplate content blocks and deduplicate similar content."""
        filtered = []
        for block in blocks:
            text = block.text.strip()
            if BOILERPLATE_RE.match(text):
                self.stats['boilerplate_skipped'] += 1
                logger.debug("Filtered boilerplate: '%s...'", text[:50])
                continue
            # Skip very short non-table blocks (likely headers/fragments)
            if not block.is_table and len(text.split()) < 5:
                self.stats['boilerplate_skipped'] += 1
                continue
            
            # Content-based deduplication
            fingerprint = self._get_content_fingerprint(text)
            if fingerprint in self._seen_content:
                self.stats['content_deduped'] += 1
                logger.debug("Deduped content block: '%s...'", text[:50])
                continue
            if len(fingerprint) > 20:  # Only track substantial content
                self._seen_content.add(fingerprint)
            
            filtered.append(block)
        return filtered
    
    def _get_content_fingerprint(self, text: str) -> str:
        """Get a normalized fingerprint of content for deduplication.
        
        Strips table formatting, normalizes whitespace, and lowercases
        to catch near-duplicate content.
        """
        # For tables, strip markdown formatting
        if '|' in text:
            text = self.TABLE_DATA_RE.sub(' ', text)
        # Normalize: lowercase, single spaces, strip punctuation
        text = re.sub(r'[^a-z0-9\s]', '', text.lower())
        text = ' '.join(text.split())
        # Return first 200 chars as fingerprint (enough to identify unique content)
        return text[:200]
    
    def _extract_sections(self, blocks: list[Block]) -> list[Section]:
        """Group blocks into sections by ITEM headers.
        
        Uses block.section_hint if available (from parser), otherwise
        falls back to regex pattern matching.
        """
        sections, current = [], None
        preamble_blocks = []  # Blocks before first ITEM
        
        for block in blocks:
            text = block.text.strip()
            
            # Check for section_hint first (from parser's form-specific detection)
            is_header = False
            item_num = ""
            title_text = ""
            
            if hasattr(block, 'section_hint') and block.section_hint:
                # Parse hint like "item:1A" or "part:I"
                parts = block.section_hint.split(":", 1)
                if len(parts) == 2:
                    header_type, item_num = parts
                    # Track Part transitions for 10-Q
                    if self.form_type == "10-Q" and header_type == "part":
                        self._current_10q_part = item_num  # "I" or "II"
                        continue  # Part headers don't create sections
                    is_header = header_type == "item"
                    # Get title using helper method
                    title_text = self._get_item_title(item_num)
            
            # Fallback to regex matching
            if not is_header:
                match = self.pattern.match(text)
                if match and block.type in ['header', 'text', 'item_header']:
                    is_header = True
                    item_num = match.group(1).upper()
                    title_text = match.group(2).strip() if match.group(2) else ""
            
            if is_header:
                # Save previous section
                if current:
                    sections.append(current)
                elif preamble_blocks:
                    sections.append(Section(
                        id="preamble",
                        title="Cover and Table of Contents",
                        blocks=preamble_blocks,
                        start_page=preamble_blocks[0].page if preamble_blocks else 1
                    ))
                    preamble_blocks = []
                
                # Use standard title if available and parsed title is short
                if len(title_text) < 10:
                    std_title = self._get_item_title(item_num)
                    if std_title:
                        title_text = std_title
                
                current = Section(
                    id=f"item_{item_num.lower().replace('.','_')}",
                    title=f"Item {item_num}: {title_text}",
                    blocks=[],
                    start_page=block.page
                )
            elif current:
                current.blocks.append(block)
            else:
                preamble_blocks.append(block)
        
        # Don't forget the last section
        if current:
            sections.append(current)
        
        # Handle case with no ITEM headers
        if not sections and blocks:
            sections = [Section("full_doc", "Full Document", blocks, 1)]
            logger.warning("No ITEM headers found, using full document as single section")
        elif not sections and preamble_blocks:
            sections = [Section("preamble", "Document Content", preamble_blocks, 1)]
        
        return sections
    
    def _chunk_section(self, section: Section, ticker: str, year: str, accession: str) -> list[Chunk]:
        """Chunk a section into target-sized pieces with overlap."""
        chunks = []
        buffer, buf_words, buf_page, buf_table = [], 0, section.start_page, False
        last_overlap = ""  # Store overlap from previous chunk
        
        def flush(add_overlap: bool = True):
            nonlocal buffer, buf_words, buf_table, last_overlap
            if not buffer:
                return
            
            content = ' '.join(buffer)
            
            # Prepend overlap from previous chunk (except for first chunk)
            if add_overlap and last_overlap and chunks:
                content = last_overlap + " " + content
            
            # Store overlap for next chunk (last N words)
            words_list = content.split()
            if len(words_list) > self.overlap_size:
                last_overlap = ' '.join(words_list[-self.overlap_size:])
            else:
                last_overlap = ""
            
            chunks.append(Chunk(
                id=f"{ticker}_{self.form_type}_{year}_{section.id}_{len(chunks):04d}",
                content=content,
                ticker=ticker,
                form_type=self.form_type,
                year=year,
                accession_number=accession,
                section_id=section.id,
                section_title=section.title,
                page=buf_page,
                has_table=buf_table,
                word_count=len(content.split())
            ))
            buffer, buf_words, buf_table = [], 0, False
        
        for block in section.blocks:
            words = len(block.text.split())
            
            # Skip trivial tables
            if block.is_table and words < self.min_table_words:
                self.stats['trivial_skipped'] += 1
                logger.debug("Skipped trivial table (%d words) at page %d", words, block.page)
                continue
            
            # Tables get their own chunk (flush before and after)
            if block.is_table:
                flush()
                buf_page = block.page
                buffer = [block.text]
                buf_words = words
                buf_table = True
                flush(add_overlap=False)  # No overlap for tables
            # Check if adding this block exceeds max size
            elif buf_words + words > self.max_chunk_size:
                # Need to split - first flush current buffer if over target
                if buf_words >= self.target_chunk_size:
                    flush()
                    buf_page = block.page
                
                # Split block by sentences if too large
                for sent in re.split(r'(?<=[.!?])\s+', block.text):
                    sw = len(sent.split())
                    if buf_words + sw > self.max_chunk_size:
                        flush()
                        buf_page = block.page
                    buffer.append(sent)
                    buf_words += sw
            else:
                if not buffer:
                    buf_page = block.page
                buffer.append(block.text)
                buf_words += words
        
        # Flush remaining buffer
        flush()
        return chunks
    
    def _merge_undersized_chunks(self, chunks: list[Chunk]) -> list[Chunk]:
        """Merge chunks smaller than min_chunk_size with neighbors."""
        if not chunks:
            return chunks
        
        merged = []
        i = 0
        
        while i < len(chunks):
            chunk = chunks[i]
            
            # If chunk is undersized and not a table
            if chunk.word_count < self.min_chunk_size and not chunk.has_table:
                # Try to merge with next chunk in same section
                if i + 1 < len(chunks) and chunks[i + 1].section_id == chunk.section_id:
                    next_chunk = chunks[i + 1]
                    merged_content = chunk.content + " " + next_chunk.content
                    merged_words = len(merged_content.split())
                    
                    # Only merge if result isn't too large
                    if merged_words <= self.max_chunk_size:
                        merged_chunk = Chunk(
                            id=chunk.id,  # Keep first chunk's ID
                            content=merged_content,
                            ticker=chunk.ticker,
                            form_type=chunk.form_type,
                            year=chunk.year,
                            accession_number=chunk.accession_number,
                            section_id=chunk.section_id,
                            section_title=chunk.section_title,
                            page=chunk.page,
                            has_table=chunk.has_table or next_chunk.has_table,
                            word_count=merged_words
                        )
                        self.stats['merged_undersized'] += 1
                        logger.debug("Merged undersized chunk (%d words) with next (%d words) -> %d words",
                                   chunk.word_count, next_chunk.word_count, merged_words)
                        
                        # Replace in chunks list for potential further merging
                        chunks[i + 1] = merged_chunk
                        i += 1
                        continue
                
                # Try to merge with previous chunk in same section
                if merged and merged[-1].section_id == chunk.section_id and not merged[-1].has_table:
                    prev_chunk = merged[-1]
                    merged_content = prev_chunk.content + " " + chunk.content
                    merged_words = len(merged_content.split())
                    
                    if merged_words <= self.max_chunk_size:
                        merged[-1] = Chunk(
                            id=prev_chunk.id,
                            content=merged_content,
                            ticker=prev_chunk.ticker,
                            form_type=prev_chunk.form_type,
                            year=prev_chunk.year,
                            accession_number=prev_chunk.accession_number,
                            section_id=prev_chunk.section_id,
                            section_title=prev_chunk.section_title,
                            page=prev_chunk.page,
                            has_table=prev_chunk.has_table or chunk.has_table,
                            word_count=merged_words
                        )
                        self.stats['merged_undersized'] += 1
                        logger.debug("Merged undersized chunk (%d words) with previous -> %d words",
                                   chunk.word_count, merged_words)
                        i += 1
                        continue
            
            merged.append(chunk)
            i += 1
        
        return merged
