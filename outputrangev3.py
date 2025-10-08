import csv
from collections import Counter, defaultdict

def process_csvs():
    try:
        # Read output.csv
        output_data = []
        with open('output100.csv', 'r', newline='') as output_file:
            csv_reader = csv.reader(output_file)
            for row in csv_reader:
                output_data.append(row)
        
        # Extract values from columns 2, 3, and 6 (indices 1, 2, and 5)
        col2_values = []
        col3_values = []
        # Map to track which values in col2 and col3 have which character in col6
        col2_col6_map = defaultdict(set)
        col3_col6_map = defaultdict(set)
        
        for row in output_data:
            if len(row) > 1 and row[1].strip():
                try:
                    value = float(row[1])
                    col2_values.append(value)
                    
                    # Store character from col6 if it exists
                    if len(row) > 5 and row[5].strip():
                        col2_col6_map[value].add(row[5])
                except ValueError:
                    pass
            
            if len(row) > 2 and row[2].strip():
                try:
                    value = float(row[2])
                    col3_values.append(value)
                    
                    # Store character from col6 if it exists
                    if len(row) > 5 and row[5].strip():
                        col3_col6_map[value].add(row[5])
                except ValueError:
                    pass
        
        # Count occurrences in columns 2 and 3
        col2_counter = Counter(col2_values)
        col3_counter = Counter(col3_values)
        
        # Find values that appear exactly once in column 2
        unique_col2_values = {value for value, count in col2_counter.items() if count == 1}
        
        # Find values that appear exactly once in column 3
        unique_col3_values = {value for value, count in col3_counter.items() if count == 1}
        
        # Read addrangev2.csv
        addrange_data = []
        with open('addrangev2.csv', 'r', newline='') as addrange_file:
            csv_reader = csv.reader(addrange_file)
            for row in csv_reader:
                addrange_data.append(row)
        
        # Process each row in addrangev2.csv
        result = []
        matches_found = 0
        non_matches = 0
        
        for row in addrange_data:
            new_row = row.copy()
            
            # Check column 6 (index 5)
            if len(row) > 5 and row[5].strip():
                try:
                    value_to_check = float(row[5])
                    target_value = value_to_check - 3
                    
                    # Check if target_value appears exactly once in both columns 2 and 3
                    if (target_value in unique_col2_values and 
                        target_value in unique_col3_values):
                        
                        # Check if values in columns 2 and 3 share the same character in column 6
                        col2_chars = col2_col6_map.get(target_value, set())
                        col3_chars = col3_col6_map.get(target_value, set())
                        
                        # If there's any character in common between col2 and col3, it's not a match
                        common_chars = col2_chars.intersection(col3_chars)
                        
                        if not common_chars:
                            new_row.append('y')
                            matches_found += 1
                        else:
                            new_row.append('n')
                            non_matches += 1
                    else:
                        new_row.append('n')
                        non_matches += 1
                except ValueError:
                    new_row.append('n')
                    non_matches += 1
            else:
                new_row.append('n')
                non_matches += 1
            
            result.append(new_row)
        
        # Write result to a new CSV file
        with open('addrangev2_results.csv', 'w', newline='') as result_file:
            csv_writer = csv.writer(result_file)
            csv_writer.writerows(result)
        
        print(f"Processing complete!")
        print(f"Total rows processed: {len(addrange_data)}")
        print(f"Matches found: {matches_found}")
        print(f"Non-matches: {non_matches}")
        print(f"Results saved to 'addrangev2_results.csv'")
        
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