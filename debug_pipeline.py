#!/usr/bin/env python3
"""
Debug script to test the pipeline step by step
"""
import sys
import pandas as pd
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))

def test_pipeline_steps():
    """Test pipeline steps individually"""
    print("Testing pipeline steps...")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'provider_reference_id': ['PROV_001', 'PROV_002', 'PROV_003'],
        'negotiated_rate': [100.0, 150.0, 200.0],
        'billing_code': ['99213', '99214', '99215'],
        'prov_npi': ['1234567890', '1234567891', '1234567892'],
        'primary_taxonomy_desc': ['Internal Medicine', 'Cardiology', 'Hospital']
    })
    
    print("Sample data:")
    print(sample_data)
    print()
    
    # Test each step
    from transformers.categorizer import ProcedureCategorizer
    from transformers.taxonomy_filter import TaxonomyFilter
    
    # Test categorizer
    print("1. Testing categorizer...")
    categorizer = ProcedureCategorizer(chunk_size=5)
    chunks = [sample_data]
    
    try:
        categorized_chunks = list(categorizer.categorize_procedures(chunks, 'billing_code'))
        print(f"   Categorized chunks: {len(categorized_chunks)}")
        if categorized_chunks:
            print(f"   First chunk shape: {categorized_chunks[0].shape}")
            print(f"   Columns: {list(categorized_chunks[0].columns)}")
    except Exception as e:
        print(f"   Error in categorizer: {e}")
        return
    
    # Test taxonomy filter
    print("\n2. Testing taxonomy filter...")
    taxonomy_filter = TaxonomyFilter(chunk_size=5)
    
    try:
        filtered_chunks = list(taxonomy_filter.filter_by_taxonomy(categorized_chunks))
        print(f"   Filtered chunks: {len(filtered_chunks)}")
        if filtered_chunks:
            print(f"   First chunk shape: {filtered_chunks[0].shape}")
            print(f"   Columns: {list(filtered_chunks[0].columns)}")
    except Exception as e:
        print(f"   Error in taxonomy filter: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pipeline_steps()
