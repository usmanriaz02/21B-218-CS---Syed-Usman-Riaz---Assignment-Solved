# #21B-218-CS     Syed Usman Riaz   Parallel Approach

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

# Load the CSV files
students_df = pd.read_csv('students.csv')
fees_df = pd.read_csv('fees.csv')

# Ensure student_id columns are integers and trim any whitespace
students_df["student_id"] = students_df["student_id"].astype(str).str.strip().astype(int)
fees_df["student_id"] = fees_df["student_id"].astype(str).str.strip().astype(int)

# Debugging: Print unique student IDs for verification
print("Unique Student IDs in students_df:", students_df["student_id"].unique())
print("Unique Student IDs in fees_df:", fees_df["student_id"].unique())

# Preprocess fees data to find most relevant fee dates for each student
def get_most_relevant_date(group):
    date_counts = group["fee_submission_date"].value_counts()
    if all(date_counts == 1):  # If all dates are unique
        return group["fee_submission_date"].max()  # Pick the latest date
    else:
        return date_counts.idxmax()  # Pick the most frequent date

# Create a mapping of student_id to the most relevant fee date
most_relevant_dates = fees_df.groupby("student_id").apply(get_most_relevant_date).reset_index()
most_relevant_dates.columns = ["student_id", "most_relevant_date"]

# Precompute a dictionary for fast lookups of the most relevant fee dates
most_relevant_dates_dict = most_relevant_dates.set_index('student_id')['most_relevant_date'].to_dict()

# Parallelized function to process each student
def process_student(student_row):
    student_id = student_row["student_id"]

    if pd.notna(student_id):  # Ensure the student ID is valid
        # Check if the student ID exists in the precomputed relevant dates dictionary
        most_relevant_date = most_relevant_dates_dict.get(student_id)

        if most_relevant_date:
            return f"Student ID {student_id}: Most relevant date of payment: {most_relevant_date}"
        else:
            return f"Student ID {student_id}: No fee records found."
    else:
        return f"Invalid Student ID: {student_id}"

# Execute processing in parallel
if __name__ == "__main__":
    start_time = time.time()

    # Convert students dataframe to list of dictionaries (rows) for parallel processing
    student_rows = students_df.to_dict("records")

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_student, student_rows))

    for result in results:
        print(result)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Time taken for execution: {execution_time:.4f} seconds")
