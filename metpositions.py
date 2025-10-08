import csv
from collections import Counter

def process_csvs():
    try:
        # Read metbases100.csv
        output_data = []
        with open('metbases100.csv', 'r', newline='') as output_file:
            csv_reader = csv.reader(output_file)
            for row in csv_reader:
                output_data.append(row)
        
        # Extract values from columns 2 and 3 (indices 1 and 2)
        col2_values = []
        col3_values = []
        
        for row in output_data:
            if len(row) > 1 and row[1].strip():
                try:
                    col2_values.append(float(row[1]))
                except ValueError:
                    pass
            
            if len(row) > 2 and row[2].strip():
                try:
                    col3_values.append(float(row[2]))
                except ValueError:
                    pass
        
        # Count occurrences in columns 2 and 3
        col2_counter = Counter(col2_values)
        col3_counter = Counter(col3_values)
        
        # Find values that appear exactly once in column 2
        unique_col2_values = {value for value, count in col2_counter.items() if count == 1}
        
        # Find values that appear exactly once in column 3
        unique_col3_values = {value for value, count in col3_counter.items() if count == 1}
        
        # Find values that appear exactly once in BOTH columns 2 and 3
        common_unique_values = unique_col2_values.intersection(unique_col3_values)
        
        # Read gRNAlist.csv
        addrange_data = []
        with open('gRNAlist.csv', 'r', newline='') as addrange_file:
            csv_reader = csv.reader(addrange_file)
            for row in csv_reader:
                addrange_data.append(row)
        
        # Process each row in gRNAlist.csv and find matches
        result = []
        
        for row in addrange_data:
            if len(row) > 5:  # Ensure row has at least 6 columns
                try:
                    # Get the range start and end (5th and 6th columns, indices 4 and 5)
                    range_start = float(row[4]) if row[4].strip() else None
                    range_end = float(row[5]) if row[5].strip() else None
                    
                    if range_start is not None and range_end is not None:
                        # Find all values that appear exactly once in BOTH columns and fall within this range
                        matches = []
                        for value in common_unique_values:
                            if range_start <= value <= range_end:
                                # Calculate offset from range_start
                                offset = value - range_start
                                matches.append(offset)
                        
                        # Create a row for the result regardless of whether matches were found
                        # Sort matches for better readability
                        matches.sort()
                        
                        # Format as comma-separated string
                        matches_str = ','.join(map(str, matches))
                        
                        # Add row to result: [col3, col5, col6, matches]
                        col3_value = row[2] if len(row) > 2 else ""
                        new_row = [col3_value, str(range_start), str(range_end), matches_str]
                        result.append(new_row)
                        
                except ValueError:
                    # Skip rows with non-numeric values in range columns
                    pass
        
        # Write result to a new CSV file
        with open('range_matches.csv', 'w', newline='') as result_file:
            csv_writer = csv.writer(result_file)
            csv_writer.writerows(result)
        
        print(f"Processing complete!")
        print(f"Total valid ranges processed: {len(result)}")
        print(f"Ranges with matches: {len([r for r in result if r[3]])}")
        print(f"Results saved to 'range_matches.csv'")
        
        # Display first few rows as preview
        print("\nFirst few rows of the result:")
        for i, row in enumerate(result[:5]):
            print(row)
            if i >= 4:  # Show only 5 rows
                break
    
    except Exception as e:
        print(f"Error processing CSV files: {str(e)}")

# Execute the function
if __name__ == "__main__":
    process_csvs()