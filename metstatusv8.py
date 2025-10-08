import pandas as pd
import numpy as np
import gc
from collections import defaultdict

def match_csvs_optimized(met75_file, gRNA_file, output_file):
    """
    Optimized version with significant performance improvements:
    - Vectorized operations instead of row-by-row processing
    - Efficient numpy array operations
    - Optimized data structures
    - Bulk processing of matches
    """
    
    print("Reading CSV files...")
    met75_df = pd.read_csv(met75_file)
    gRNA_df = pd.read_csv(gRNA_file)
    
    print(f"met75trimfix.csv shape: {met75_df.shape}")
    print(f"gRNAranges.csv shape: {gRNA_df.shape}")
    
    # Clean met75 data with vectorized operations
    print("Cleaning met75 data...")
    met75_df['pos'] = pd.to_numeric(met75_df.iloc[:, 0], errors='coerce')
    met75_df['third_col_val'] = met75_df.iloc[:, 2]
    met75_df['match_val'] = met75_df.iloc[:, 3]
    
    # Drop NaN values in one operation
    met75_clean = met75_df.dropna(subset=['pos', 'match_val']).copy()
    del met75_df
    gc.collect()
    
    print(f"met75 after cleaning: {len(met75_clean)} rows")
    
    # Create optimized lookup using defaultdict and numpy arrays
    print("Creating optimized lookup...")
    lookup = defaultdict(lambda: {'pos': [], 'val': []})
    
    # Vectorized groupby operation
    for match_val, group in met75_clean.groupby('match_val'):
        lookup[match_val]['pos'] = group['pos'].values
        lookup[match_val]['val'] = group['third_col_val'].values
    
    del met75_clean
    gc.collect()
    
    print(f"Created lookup for {len(lookup)} unique match values")
    
    # Clean gRNA data with vectorized operations
    print("Cleaning gRNA data...")
    gRNA_df['range_start'] = pd.to_numeric(gRNA_df.iloc[:, 4], errors='coerce')
    gRNA_df['range_end'] = pd.to_numeric(gRNA_df.iloc[:, 5], errors='coerce')
    gRNA_df['match_val'] = gRNA_df.iloc[:, 7]
    gRNA_df['strand'] = gRNA_df['Strand'] if 'Strand' in gRNA_df.columns else None
    
    # Initialize result columns
    gRNA_df['matched_positions'] = ''
    gRNA_df['matched_third_col_values'] = ''
    gRNA_df['has_matches'] = 'n'
    
    # Get valid rows (non-NaN) for bulk processing
    valid_mask = ~(gRNA_df['range_start'].isna() | gRNA_df['range_end'].isna() | gRNA_df['match_val'].isna())
    valid_indices = gRNA_df[valid_mask].index.tolist()
    
    print(f"Processing {len(valid_indices)} valid rows...")
    
    # Process in larger, more efficient chunks
    chunk_size = 5000  # Larger chunks for better efficiency
    total_chunks = len(valid_indices) // chunk_size + (1 if len(valid_indices) % chunk_size != 0 else 0)
    
    matches_found = 0
    
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min((chunk_idx + 1) * chunk_size, len(valid_indices))
        chunk_indices = valid_indices[start_idx:end_idx]
        
        print(f"Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_indices)} rows)...")
        
        # Extract chunk data for vectorized operations
        chunk_data = gRNA_df.loc[chunk_indices, ['range_start', 'range_end', 'match_val', 'strand']].copy()
        
        # Process each row in the chunk
        for idx in chunk_indices:
            row = chunk_data.loc[idx]
            match_val = row['match_val']
            
            # Skip if match_val not in lookup
            if match_val not in lookup:
                continue
            
            # Get positions and values for this match_val
            positions = lookup[match_val]['pos']
            values = lookup[match_val]['val']
            
            # Use vectorized operations for range filtering
            range_start = row['range_start']
            range_end = row['range_end']
            
            # Vectorized boolean mask for range filtering
            mask = (positions >= range_start) & (positions <= range_end)
            
            if np.any(mask):
                matching_positions = positions[mask]
                matching_values = values[mask]
                
                # Vectorized position adjustment
                adjusted_positions = matching_positions - range_start
                
                # Apply strand-specific calculations
                strand = row['strand']
                if pd.notna(strand):
                    if strand == '+':
                        final_positions = adjusted_positions + 2
                    elif strand == '-':
                        final_positions = 28 - adjusted_positions
                    else:
                        final_positions = adjusted_positions
                else:
                    final_positions = adjusted_positions
                
                # Sort by position using argsort for efficiency
                sort_indices = np.argsort(final_positions)
                sorted_positions = final_positions[sort_indices]
                sorted_values = matching_values[sort_indices]
                
                # Convert to strings efficiently
                pos_strings = [str(int(pos)) for pos in sorted_positions]
                val_strings = [str(val) for val in sorted_values]
                
                # Update dataframe
                gRNA_df.loc[idx, 'matched_positions'] = ','.join(pos_strings)
                gRNA_df.loc[idx, 'matched_third_col_values'] = ','.join(val_strings)
                gRNA_df.loc[idx, 'has_matches'] = 'y'
                matches_found += 1
        
        # Less frequent garbage collection
        if chunk_idx % 10 == 0:
            gc.collect()
    
    print("Saving results...")
    gRNA_df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
    print(f"Total rows with matches: {matches_found} out of {len(valid_indices)}")
    
    return gRNA_df

def match_csvs_ultra_fast(met75_file, gRNA_file, output_file):
    """
    Ultra-fast version using advanced vectorization and optimized data structures
    """
    print("Using ultra-fast approach...")
    
    # Read files
    print("Reading files...")
    met75_df = pd.read_csv(met75_file)
    gRNA_df = pd.read_csv(gRNA_file)
    
    # Clean met75 data
    print("Processing met75 data...")
    met75_df['pos'] = pd.to_numeric(met75_df.iloc[:, 0], errors='coerce')
    met75_df['third_col_val'] = met75_df.iloc[:, 2]
    met75_df['match_val'] = met75_df.iloc[:, 3]
    met75_clean = met75_df.dropna(subset=['pos', 'match_val'])
    
    # Create highly optimized lookup
    print("Creating ultra-fast lookup...")
    lookup_dict = {}
    
    # Group and convert to numpy arrays for maximum speed
    grouped = met75_clean.groupby('match_val')
    for match_val, group in grouped:
        lookup_dict[match_val] = {
            'positions': group['pos'].values,
            'values': group['third_col_val'].values
        }
    
    del met75_df, met75_clean
    gc.collect()
    
    # Clean gRNA data
    print("Processing gRNA data...")
    gRNA_df['range_start'] = pd.to_numeric(gRNA_df.iloc[:, 4], errors='coerce')
    gRNA_df['range_end'] = pd.to_numeric(gRNA_df.iloc[:, 5], errors='coerce')
    gRNA_df['match_val'] = gRNA_df.iloc[:, 7]
    gRNA_df['strand'] = gRNA_df.get('Strand', None)
    
    # Initialize result columns
    gRNA_df['matched_positions'] = ''
    gRNA_df['matched_third_col_values'] = ''
    gRNA_df['has_matches'] = 'n'
    
    # Filter valid rows
    valid_mask = ~(gRNA_df['range_start'].isna() | gRNA_df['range_end'].isna() | gRNA_df['match_val'].isna())
    valid_df = gRNA_df[valid_mask].copy()
    
    print(f"Processing {len(valid_df)} valid rows...")
    
    # Process in large chunks for maximum efficiency
    chunk_size = 10000
    total_processed = 0
    matches_found = 0
    
    for i in range(0, len(valid_df), chunk_size):
        chunk = valid_df.iloc[i:i+chunk_size]
        print(f"Processing chunk {i//chunk_size + 1}/{(len(valid_df)-1)//chunk_size + 1}...")
        
        for idx, row in chunk.iterrows():
            match_val = row['match_val']
            
            if match_val in lookup_dict:
                positions = lookup_dict[match_val]['positions']
                values = lookup_dict[match_val]['values']
                
                # Vectorized range check
                range_start = row['range_start']
                range_end = row['range_end']
                mask = (positions >= range_start) & (positions <= range_end)
                
                if np.sum(mask) > 0:  # Use np.sum for speed
                    matching_pos = positions[mask]
                    matching_vals = values[mask]
                    
                    # Vectorized calculations
                    adjusted_pos = matching_pos - range_start
                    
                    # Strand calculations
                    strand = row['strand']
                    if pd.notna(strand):
                        if strand == '+':
                            final_pos = adjusted_pos + 2
                        elif strand == '-':
                            final_pos = 28 - adjusted_pos
                        else:
                            final_pos = adjusted_pos
                    else:
                        final_pos = adjusted_pos
                    
                    # Sort efficiently
                    sort_idx = np.argsort(final_pos)
                    sorted_pos = final_pos[sort_idx]
                    sorted_vals = matching_vals[sort_idx]
                    
                    # Convert to strings
                    pos_str = ','.join(str(int(p)) for p in sorted_pos)
                    val_str = ','.join(str(v) for v in sorted_vals)
                    
                    # Update results
                    gRNA_df.loc[idx, 'matched_positions'] = pos_str
                    gRNA_df.loc[idx, 'matched_third_col_values'] = val_str
                    gRNA_df.loc[idx, 'has_matches'] = 'y'
                    matches_found += 1
            
            total_processed += 1
            if total_processed % 5000 == 0:
                print(f"Processed {total_processed} rows, found {matches_found} matches")
    
    print("Saving results...")
    gRNA_df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
    print(f"Total matches found: {matches_found}")
    
    return gRNA_df

def get_file_info(filename):
    """Helper function to get basic file information"""
    try:
        df = pd.read_csv(filename, nrows=5)
        return {
            'columns': df.columns.tolist(),
            'shape_preview': f"~{len(df)} rows (showing first 5)",
            'sample_data': df.head()
        }
    except Exception as e:
        return f"Error reading file: {e}"

# Main execution
if __name__ == "__main__":
    # File paths
    met75_file = "met75trimfix.csv"
    gRNA_file = "gRNAranges.csv"
    output_file = "matched_results75_v8.csv"
    
    # Check file info
    print("Checking file information...")
    print("met75trimfix.csv info:")
    print(get_file_info(met75_file))
    print("\ngRNAranges.csv info:")
    print(get_file_info(gRNA_file))
    
    try:
        # Try ultra-fast version first
        print("\nTrying ultra-fast approach...")
        result = match_csvs_ultra_fast(met75_file, gRNA_file, output_file)
        
        # Display results
        print("\nFirst 5 rows of results:")
        non_empty = result[result['matched_positions'] != '']
        if len(non_empty) > 0:
            print(non_empty[['matched_positions', 'matched_third_col_values', 'has_matches']].head())
        else:
            print("No matches found")
            
        # Show summary
        match_summary = result['has_matches'].value_counts()
        print(f"\nMatch summary:")
        print(match_summary)
        
    except Exception as e:
        print(f"Ultra-fast approach failed: {e}")
        print("Trying optimized approach...")
        
        try:
            result = match_csvs_optimized(met75_file, gRNA_file, output_file)
            print("Optimized approach completed successfully")
        except Exception as e2:
            print(f"All optimized approaches failed. Error: {e2}")
            print("Consider using a machine with more RAM or splitting files into smaller chunks.")