#!/usr/bin/env python3
"""
Debug script to test the categorizer
"""
import sys
import pandas as pd
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))

def test_categorizer():
    """Test the categorizer with sample data"""
    print("Testing categorizer...")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'billing_code': ['99213', '99214', '99215', '99281', '99282'],
        'rate': [100.0, 150.0, 200.0, 250.0, 300.0]
    })
    
    print("Sample data:")
    print(sample_data)
    print()
    
    # Test categorizer
    from transformers.categorizer import ProcedureCategorizer
    
    categorizer = ProcedureCategorizer(chunk_size=5)
    
    # Test with a single chunk
    chunks = [sample_data]
    
    try:
        result_chunks = list(categorizer.categorize_procedures(chunks, 'billing_code'))
        print(f"Number of result chunks: {len(result_chunks)}")
        
        if result_chunks:
            result = result_chunks[0]
            print("Categorized data:")
            print(result)
            print(f"Columns: {list(result.columns)}")
        else:
            print("No chunks returned from categorizer")
            
    except Exception as e:
        print(f"Error in categorizer: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_categorizer()
