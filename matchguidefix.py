import csv
import os

def process_csv(input_file, output_file):
    # Read the CSV file
    with open(input_file, 'r', newline='') as infile:
        reader = csv.reader(infile)
        rows = list(reader)
    
    # Process each row
    new_rows = []
    for row in rows:
        # Check if there's a value in the 4th column (index 3)
        has_value = 'y' if (len(row) > 3 and row[3].strip() != '') else 'n'
        
        # Create new row with the new column added at the beginning
        new_row = [has_value] + row
        new_rows.append(new_row)
    
    # Write the modified data to a new CSV file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(new_rows)
    
    print(f"Processed {len(rows)} rows and saved to {output_file}")

if __name__ == "__main__":
    input_file = "range_matches.csv"
    output_file = "range_matches_modified.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
    else:
        process_csv(input_file, output_file)