import pandas as pd
import sys

def filter_csv(input_file, output_file=None):
    """
    Filter CSV file by removing rows where the 10th column has values < 10
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file (optional)
    """
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Check if CSV has at least 10 columns
        if len(df.columns) < 10:
            print(f"Error: CSV file only has {len(df.columns)} columns. Need at least 10.")
            return
        
        # Get the 10th column (index 9 since pandas uses 0-based indexing)
        tenth_column = df.columns[9]
        
        print(f"Original dataset: {len(df)} rows")
        print(f"Filtering by column '{tenth_column}' (10th column)")
        
        # Convert 10th column to numeric, handling non-numeric values
        df[tenth_column] = pd.to_numeric(df[tenth_column], errors='coerce')
        
        # Filter out rows where 10th column is less than 10
        # Also removes rows where the value couldn't be converted to numeric (NaN)
        filtered_df = df[df[tenth_column] >= 10]
        
        print(f"Filtered dataset: {len(filtered_df)} rows")
        print(f"Removed {len(df) - len(filtered_df)} rows")
        
        # Save to output file
        if output_file is None:
            output_file = input_file.replace('.csv', '_filtered.csv')
        
        filtered_df.to_csv(output_file, index=False)
        print(f"Filtered data saved to: {output_file}")
        
        return filtered_df
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"Error processing file: {str(e)}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <input_csv_file> [output_csv_file]")
        print("Example: python script.py data.csv")
        print("Example: python script.py data.csv filtered_data.csv")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    filter_csv(input_file, output_file)

if __name__ == "__main__":
    main()