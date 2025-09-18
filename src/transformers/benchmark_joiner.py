"""
Benchmark Joiner - Applies Medicare benchmarks after dataset filtering based on billing_class
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class BenchmarkJoiner:
    """Join Medicare benchmarks with filtered dataset based on billing_class"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
        self.benchmark_tables = None
        self.benchmark_dir = Path("prod_etl/core/data/silver/benchmarks")
        
    def load_benchmark_tables(self) -> Dict[str, pd.DataFrame]:
        """Load all Medicare benchmark tables once for efficiency"""
        if self.benchmark_tables is not None:
            return self.benchmark_tables
            
        logger.info("Loading Medicare benchmark tables...")
        
        if not self.benchmark_dir.exists():
            logger.error(f"Benchmark directory not found: {self.benchmark_dir}")
            logger.info("Run 'python build_medicare_benchmarks.py' first to create benchmark tables")
            return {}
        
        try:
            # Load individual benchmark tables
            prof_bench = pd.read_parquet(self.benchmark_dir / "bench_medicare_professional.parquet")
            opps_bench = pd.read_parquet(self.benchmark_dir / "bench_medicare_opps.parquet")
            asc_bench = pd.read_parquet(self.benchmark_dir / "bench_medicare_asc.parquet")
            
            self.benchmark_tables = {
                'professional': prof_bench,
                'opps': opps_bench,
                'asc': asc_bench
            }
            
            logger.info(f"Loaded benchmark tables:")
            logger.info(f"  Professional: {len(prof_bench):,} rows")
            logger.info(f"  OPPS: {len(opps_bench):,} rows")
            logger.info(f"  ASC: {len(asc_bench):,} rows")
            
            return self.benchmark_tables
            
        except FileNotFoundError as e:
            logger.error(f"Benchmark table not found: {e}")
            logger.info("Run 'python build_medicare_benchmarks.py' first to create benchmark tables")
            return {}
    
    def join_benchmarks_with_filtered_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Join Medicare benchmarks with filtered dataset based on billing_class"""
        logger.info("Joining Medicare benchmarks with filtered dataset...")
        
        # Load benchmark tables once
        benchmark_tables = self.load_benchmark_tables()
        if not benchmark_tables:
            logger.warning("No benchmark tables available - returning chunks unchanged")
            return chunks
        
        def join_benchmarks_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if chunk.empty:
                return chunk
                
            # Check required columns
            required_cols = ['billing_class', 'state', 'year_month', 'code_type', 'code']
            missing_cols = [col for col in required_cols if col not in chunk.columns]
            if missing_cols:
                logger.warning(f"Missing required columns for benchmark joining: {missing_cols}")
                return chunk
            
            # Create a copy to avoid modifying original
            result_chunk = chunk.copy()
            
            # Process professional billing_class
            professional_mask = result_chunk['billing_class'] == 'professional'
            if professional_mask.any():
                logger.debug(f"Joining professional benchmarks for {professional_mask.sum()} rows")
                result_chunk = self._join_professional_benchmarks(
                    result_chunk, professional_mask, benchmark_tables['professional']
                )
            
            # Process institutional billing_class (OPPS and ASC)
            institutional_mask = result_chunk['billing_class'] == 'institutional'
            if institutional_mask.any():
                logger.debug(f"Joining institutional benchmarks for {institutional_mask.sum()} rows")
                result_chunk = self._join_institutional_benchmarks(
                    result_chunk, institutional_mask, benchmark_tables['opps'], benchmark_tables['asc']
                )
            
            return result_chunk
        
        return self.chunk_processor.filter_chunks(chunks, join_benchmarks_chunk)
    
    def _join_professional_benchmarks(self, chunk: pd.DataFrame, mask: pd.Series, 
                                    prof_bench: pd.DataFrame) -> pd.DataFrame:
        """Join professional Medicare benchmarks"""
        # Filter chunk to only professional records
        prof_chunk = chunk[mask].copy()
        
        # Join with professional benchmarks
        prof_joined = prof_chunk.merge(
            prof_bench,
            on=['state', 'year_month', 'code_type', 'code'],
            how='left',
            suffixes=('', '_bench')
        )
        
        # Add benchmark columns to the main chunk
        benchmark_cols = [
            'medicare_prof_national', 'medicare_prof_stateavg',
            'work_rvu', 'practice_expense_rvu', 'malpractice_rvu',
            'conversion_factor', 'benchmark_type', 'created_date', 'data_year'
        ]
        
        for col in benchmark_cols:
            if col in prof_joined.columns:
                chunk.loc[mask, col] = prof_joined[col].values
        
        # Calculate percentage of Medicare for professional rates
        if 'medicare_prof_stateavg' in chunk.columns and 'rate' in chunk.columns:
            chunk.loc[mask, 'pct_of_medicare'] = (
                chunk.loc[mask, 'rate'] / chunk.loc[mask, 'medicare_prof_stateavg']
            )
        
        return chunk
    
    def _join_institutional_benchmarks(self, chunk: pd.DataFrame, mask: pd.Series,
                                     opps_bench: pd.DataFrame, asc_bench: pd.DataFrame) -> pd.DataFrame:
        """Join institutional Medicare benchmarks (OPPS and ASC)"""
        # Filter chunk to only institutional records
        inst_chunk = chunk[mask].copy()
        
        # Join with OPPS benchmarks
        opps_joined = inst_chunk.merge(
            opps_bench,
            on=['state', 'year_month', 'code_type', 'code'],
            how='left',
            suffixes=('', '_opps')
        )
        
        # Join with ASC benchmarks
        asc_joined = opps_joined.merge(
            asc_bench,
            on=['state', 'year_month', 'code_type', 'code'],
            how='left',
            suffixes=('', '_asc')
        )
        
        # Add OPPS benchmark columns to the main chunk
        opps_cols = [
            'medicare_opps_national', 'medicare_opps_stateavg',
            'opps_weight', 'opps_si', 'opps_short_desc',
            'state_wage_index_avg', 'opps_adj_factor_stateavg'
        ]
        
        for col in opps_cols:
            if col in asc_joined.columns:
                chunk.loc[mask, col] = asc_joined[col].values
        
        # Add ASC benchmark columns to the main chunk
        asc_cols = [
            'medicare_asc_national', 'medicare_asc_stateavg',
            'asc_pi', 'asc_nat_rate', 'asc_short_desc',
            'asc_adj_factor_stateavg'
        ]
        
        for col in asc_cols:
            if col in asc_joined.columns:
                chunk.loc[mask, col] = asc_joined[col].values
        
        # Add benchmark type and metadata
        chunk.loc[mask, 'benchmark_type'] = 'institutional'
        if 'created_date' in asc_joined.columns:
            chunk.loc[mask, 'created_date'] = asc_joined['created_date'].values
        if 'data_year' in asc_joined.columns:
            chunk.loc[mask, 'data_year'] = asc_joined['data_year'].values
        
        # Calculate percentage of Medicare for institutional rates
        if 'rate' in chunk.columns:
            # Use OPPS state average as primary benchmark for institutional
            if 'medicare_opps_stateavg' in chunk.columns:
                chunk.loc[mask, 'pct_of_medicare_opps'] = (
                    chunk.loc[mask, 'rate'] / chunk.loc[mask, 'medicare_opps_stateavg']
                )
            
            # Use ASC state average as secondary benchmark for institutional
            if 'medicare_asc_stateavg' in chunk.columns:
                chunk.loc[mask, 'pct_of_medicare_asc'] = (
                    chunk.loc[mask, 'rate'] / chunk.loc[mask, 'medicare_asc_stateavg']
                )
        
        return chunk
    
    def get_benchmark_coverage_stats(self, chunks: Iterator[pd.DataFrame]) -> Dict[str, Any]:
        """Get statistics on benchmark coverage"""
        logger.info("Calculating benchmark coverage statistics...")
        
        total_rows = 0
        professional_rows = 0
        institutional_rows = 0
        prof_benchmarked = 0
        inst_benchmarked = 0
        
        for chunk in chunks:
            if chunk.empty:
                continue
                
            total_rows += len(chunk)
            
            if 'billing_class' in chunk.columns:
                professional_rows += (chunk['billing_class'] == 'professional').sum()
                institutional_rows += (chunk['billing_class'] == 'institutional').sum()
                
                if 'medicare_prof_stateavg' in chunk.columns:
                    prof_benchmarked += chunk[
                        (chunk['billing_class'] == 'professional') & 
                        chunk['medicare_prof_stateavg'].notna()
                    ].shape[0]
                
                if 'medicare_opps_stateavg' in chunk.columns:
                    inst_benchmarked += chunk[
                        (chunk['billing_class'] == 'institutional') & 
                        chunk['medicare_opps_stateavg'].notna()
                    ].shape[0]
        
        stats = {
            'total_rows': total_rows,
            'professional_rows': professional_rows,
            'institutional_rows': institutional_rows,
            'prof_benchmarked': prof_benchmarked,
            'inst_benchmarked': inst_benchmarked,
            'prof_coverage_pct': (prof_benchmarked / professional_rows * 100) if professional_rows > 0 else 0,
            'inst_coverage_pct': (inst_benchmarked / institutional_rows * 100) if institutional_rows > 0 else 0
        }
        
        logger.info(f"Benchmark coverage statistics:")
        logger.info(f"  Total rows: {total_rows:,}")
        logger.info(f"  Professional rows: {professional_rows:,} ({stats['prof_coverage_pct']:.1f}% benchmarked)")
        logger.info(f"  Institutional rows: {institutional_rows:,} ({stats['inst_coverage_pct']:.1f}% benchmarked)")
        
        return stats




