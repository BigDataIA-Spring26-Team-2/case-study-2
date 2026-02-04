"""SEC Pipeline: Snowflake -> S3 -> SEC EDGAR with intelligent fallback."""
import logging
import json
import hashlib
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from sec_edgar_downloader import Downloader

from app.pipelines.sec_parser import SECParser
from app.pipelines.sec_chunker import SECChunker
from app.services.evidence_storage import EvidenceStorage
from app.core.config_loader import get_filing_config

logger = logging.getLogger(__name__)


class SECIntegration:
    
    def __init__(self, snowflake_conn, s3_bucket: str, email: str):
        self.storage = EvidenceStorage(snowflake_conn)
        self.parser = SECParser()
        self.chunker = SECChunker()
        self.s3 = boto3.client('s3')
        self.bucket = s3_bucket
        self.downloader = Downloader("PE-OrgAIR", email, Path("data/temp"))
        
        filing_config = get_filing_config()
        self.date_cutoff = filing_config['date_cutoff']
    
    def process_company(
        self,
        ticker: str,
        company_id: str,
        filing_types: list = None,
        limit: int = None
    ) -> dict:
        if filing_types is None:
            filing_config = get_filing_config()
            filing_types = filing_config['types']
            limit = limit or filing_config['default_limit']
        
        stats = {'documents': 0, 'chunks': 0, 'skipped_db': 0, 'from_s3': 0, 'from_sec': 0, 'errors': 0}
        
        for filing_type in filing_types:
            logger.info(f"Processing {ticker} {filing_type} (limit={limit})")
            
            try:
                self.downloader.get(filing_type, ticker, limit=limit, after=self.date_cutoff)
                
                filing_dir = Path(f"data/temp/sec-edgar-filings/{ticker}/{filing_type}")
                if not filing_dir.exists():
                    continue
                
                files = list(filing_dir.glob("**/full-submission.txt"))
                logger.info(f"Found {len(files)} filings")
                
                for idx, file_path in enumerate(files, 1):
                    accession = file_path.parts[-2]
                    logger.info(f"[{idx}/{len(files)}] {accession}")
                    
                    try:
                        result = self._process_filing(file_path, company_id, ticker, filing_type, accession)
                        
                        if result['source'] == 'skipped':
                            stats['skipped_db'] += 1
                        elif result['source'] == 's3':
                            stats['from_s3'] += 1
                            stats['documents'] += 1
                            stats['chunks'] += result['chunks']
                        else:
                            stats['from_sec'] += 1
                            stats['documents'] += 1
                            stats['chunks'] += result['chunks']
                        
                        logger.info(f"  {result['source']}: {result['chunks']} chunks")
                        
                    except Exception as e:
                        logger.error(f"  Error: {e}")
                        stats['errors'] += 1
                    finally:
                        if file_path.exists():
                            file_path.unlink()
                
            except Exception as e:
                logger.error(f"Failed {filing_type}: {e}")
                stats['errors'] += 1
        
        self._cleanup_temp_dir()
        return stats
    
    def _process_filing(
        self,
        file_path: Path,
        company_id: str,
        ticker: str,
        filing_type: str,
        accession: str
    ) -> dict:
        
        if self._in_snowflake(accession):
            return {'source': 'skipped', 'chunks': 0}
        
        s3_key = f"sec/{ticker}/{filing_type}/{accession}.txt"
        
        if self._in_s3(s3_key):
            logger.debug("  Retrieving from S3")
            content = self._download_from_s3(s3_key)
            source = 's3'
        else:
            logger.debug("  Reading local, uploading to S3")
            content = file_path.read_text(encoding='utf-8', errors='ignore')
            self._upload_to_s3(content, s3_key)
            source = 'sec'
        
        logger.debug("  Parsing")
        blocks = self.parser.parse(content, form_type=filing_type)
        
        logger.debug("  Chunking")
        chunks = self.chunker.process(blocks, filing_type, accession, ticker, year="2024")
        
        logger.debug("  Storing")
        self._store_to_snowflake(chunks, company_id, ticker, filing_type, accession, s3_key)
        
        return {'source': source, 'chunks': len(chunks)}
    
    def _in_snowflake(self, accession: str) -> bool:
        cursor = self.storage.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM evidence_documents WHERE accession_number = %s", (accession,))
        return cursor.fetchone()[0] > 0
    
    def _in_s3(self, s3_key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError:
            return False
    
    def _download_from_s3(self, s3_key: str) -> str:
        response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
        return response['Body'].read().decode('utf-8')
    
    def _upload_to_s3(self, content: str, s3_key: str):
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=content.encode('utf-8'),
            ContentType='text/plain'
        )
    
    def _store_to_snowflake(
        self,
        chunks: list,
        company_id: str,
        ticker: str,
        filing_type: str,
        accession: str,
        s3_key: str
    ):
        sections_summary = {}
        for chunk in chunks:
            if chunk.section_id:
                if chunk.section_id not in sections_summary:
                    sections_summary[chunk.section_id] = {'chunk_count': 0, 'total_words': 0}
                sections_summary[chunk.section_id]['chunk_count'] += 1
                sections_summary[chunk.section_id]['total_words'] += chunk.word_count
        
        total_chunks = len(chunks)
        total_words = sum(c.word_count for c in chunks)
        section_count = len(sections_summary)
        table_count = sum(1 for c in chunks if c.has_table)
        content_hash = hashlib.sha256(str(chunks).encode()).hexdigest()[:16]
        
        doc_id = str(uuid4())
        cursor = self.storage.conn.cursor()
        
        cursor.execute("""
            INSERT INTO evidence_documents (
                id, company_id, ticker, filing_type, filing_date,
                accession_number, content_hash, s3_key, total_chunks,
                total_words, section_count, table_count, sections_summary, status
            ) SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s
        """, (
            doc_id, company_id, ticker.upper(), filing_type.upper(),
            datetime(2024, 1, 1).date(), accession, content_hash, s3_key,
            total_chunks, total_words, section_count, table_count,
            json.dumps(sections_summary), 'parsed'
        ))
        
        batch_size = 100
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            for j, chunk in enumerate(batch):
                cursor.execute("""
                    INSERT INTO document_chunks (
                        id, document_id, chunk_index, section_id, section_title,
                        content, word_count, has_table, page
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid4()), doc_id, i + j, chunk.section_id, chunk.section_title,
                    chunk.content, chunk.word_count, chunk.has_table, chunk.page
                ))
            self.storage.conn.commit()
        
        self.storage._update_company_summary(company_id, ticker)
    
    def _cleanup_temp_dir(self):
        temp_dir = Path("data/temp")
        if temp_dir.exists():
            import shutil
            shutil.rmtree(temp_dir)
            logger.debug("Cleaned temp directory")