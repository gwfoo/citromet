import csv
import os

def remove_columns(input_file, columns_to_remove, output_file=None):
    """
    Remove specified columns from a CSV file.
    
    Args:
        input_file (str): Path to the input CSV file
        columns_to_remove (list): List of column numbers to remove (1-based indexing)
        output_file (str): Path for the output file (optional)
    """
    
    # Generate default output filename if not provided
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        ext = os.path.splitext(input_file)[1]
        output_file = f"{base_name}_filtered{ext}"
    
    # Convert to 0-based indexing and sort in descending order
    # This ensures we remove columns from right to left to avoid index shifting
    columns_to_remove_indexed = sorted([col - 1 for col in columns_to_remove], reverse=True)
    
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            
            # Read all rows
            rows = list(reader)
            
            if not rows:
                print("Warning: Input file is empty.")
                return
            
            # Check if we have enough columns
            max_cols = max(len(row) for row in rows)
            invalid_columns = [col for col in columns_to_remove if col > max_cols]
            
            if invalid_columns:
                print(f"Warning: The following column numbers don't exist in the CSV: {invalid_columns}")
                print(f"CSV has {max_cols} columns maximum.")
            
            # Remove specified columns from each row
            filtered_rows = []
            for row in rows:
                # Create a copy of the row to modify
                filtered_row = row[:]
                
                # Remove columns (from right to left to avoid index issues)
                for col_idx in columns_to_remove_indexed:
                    if col_idx < len(filtered_row):
                        filtered_row.pop(col_idx)
                
                filtered_rows.append(filtered_row)
            
            # Write the filtered data
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                writer.writerows(filtered_rows)
            
            print(f"Successfully removed columns {columns_to_remove} from CSV:")
            print(f"  - Input file: {input_file}")
            print(f"  - Output file: {output_file}")
            print(f"  - Columns before: {max_cols}")
            print(f"  - Columns after: {len(filtered_rows[0]) if filtered_rows else 0}")
            
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"Error processing file: {e}")

# Example usage
if __name__ == "__main__":
    # Replace 'input.csv' with your actual file path
    input_csv = "matched_results75_v8.csv"
    
    # Columns to remove (7th, 9th, 10th, 11th)
    columns_to_remove = [7, 9, 10, 11, 12]
    
    # Option 1: Use default output name
    remove_columns(input_csv, columns_to_remove)
    
    # Option 2: Specify custom output name
    # remove_columns(input_csv, columns_to_remove, "output_filtered.csv")
    
    # Option 3: Interactive mode - uncomment to use
    # input_file = input("Enter the path to your CSV file: ")
    # remove_columns(input_file, columns_to_remove)