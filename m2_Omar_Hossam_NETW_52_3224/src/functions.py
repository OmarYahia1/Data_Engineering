import pandas as pd
import numpy as np
from sqlalchemy import create_engine


# Tidy up the column names by removing spaces and converting them to a consistent format.
def rename_columns(df):
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    return df


def new_index(df, column):
    return df.set_index(column)


def dropcol(df, column):
    return df.drop(columns=column)


def lowerstr(df, col):
    df[col] = df[col].str.lower()
    return df


#encode the 'grade' column using the categories in the dataset description (A-G).
def grade_mapping(grade):
    if 1 <= grade <= 5:
        return 'A'
    elif 6 <= grade <= 10:
        return 'B'
    elif 11 <= grade <= 15:
        return 'C'
    elif 16 <= grade <= 20:
        return 'D'
    elif 21 <= grade <= 25:
        return 'E'
    elif 26 <= grade <= 30:
        return 'F'
    elif 31 <= grade <= 35:
        return 'G'


# Function to apply the mapping to the specified column in the DataFrame.
def labelEnc(df, col, mapping):
    df[col] = df[col].map(mapping)
    return df


def map_grade(value):
    if 1 <= value <= 5:
        return 'A'
    elif 6 <= value <= 10:
        return 'B'
    elif 11 <= value <= 15:
        return 'C'
    elif 16 <= value <= 20:
        return 'D'
    elif 21 <= value <= 25:
        return 'E'
    elif 26 <= value <= 30:
        return 'F'
    elif 31 <= value <= 35:
        return 'G'
    else:
        return value


def lookup(df, original_df):
    # Initialize an empty list to store the lookup table records
    lookup_records = []
    # Create a dictionary to track seen original values for each column
    seen_values = {column: set() for column in original_df.columns}

    # Iterate over the columns to compare the original and modified data
    for column in original_df.columns:
        for original_value, modified_value in zip(original_df[column], df[column]):
            # If the original and modified values are different
            if original_value != modified_value:
                if pd.isnull(original_value):
                    original_value_display = "missing"  # Set to "missing" if original value is NaN
                else:
                    original_value_display = original_value

                # Handle special case for 'int_rate'
                if column == 'int_rate' and original_value_display == "missing":
                    modified_value = f"{modified_value} (median)"

                if (column == 'emp_length' or column == 'emp_title') and original_value_display == "missing":
                    modified_value = f"{modified_value} (mode)"
                # Check if the original value has been seen before
                if original_value_display not in seen_values[column]:
                    lookup_records.append({
                        'Column': column,
                        'Original': original_value_display,
                        'Imputed': modified_value
                    })
                    seen_values[column].add(original_value_display)  # Mark this original value as seen

            # Handle special case for 'grade'
            if column == 'grade':
                original_value_display = original_value
                modified_value = map_grade(modified_value)

                # Check if the original value has been seen before
                if original_value_display not in seen_values[column]:
                    lookup_records.append({
                        'Column': column,
                        'Original': original_value_display,
                        'Imputed': modified_value
                    })
                    seen_values[column].add(original_value_display)  # Mark this original value as seen

    # Create a DataFrame for the lookup table
    lookup_table_df = pd.DataFrame(lookup_records)
    return lookup_table_df


#a method to handle outliers using Quantile-based Flooring and Capping.
def handle_outliers(df, col):
    floor = df[col].quantile(0.10)
    cap = df[col].quantile(0.90)
    df[col] = np.where(df[col] < floor, floor, df[col])
    df[col] = np.where(df[col] > cap, cap, df[col])
    return df


#Calculate the monthly installment (M = P * r * (1 + r)^n / ((1 + r)^n - 1))
def calculate_monthly_installment(loan_amount, annual_int_rate, term_in_months):
    # Convert annual interest rate to monthly by dividing by 12.
    r = annual_int_rate / 12
    n = term_in_months  # Number of months (loan term).
    if r == 0:  # If the interest rate is zero, return the simple installment calculation (loan amount divided by term).
        return loan_amount / n
    # M = P * r * (1 + r)^n / ((1 + r)^n - 1)
    return loan_amount * r * (1 + r) * n / ((1 + r) * n - 1)


#Function that calculates the top x most frequent values in a categorical feature.
def calculate_top_categories(df, variable, how_many):
    return [
        x for x in df[variable].value_counts().sort_values(
            ascending=False).head(how_many).index
    ]


# Manually encode the most frequent values in a categorical feature.
def one_hot_encode(df, variable, top_x_labels):
    for label in top_x_labels:
        df[variable + '_' + label] = np.where(
            df[variable] == label, 1, 0)


def establish_connection(db_name):
    """Establishes a connection to the PostgreSQL database."""
    try:
        # Correct the URL format
        database_url = f"postgresql://rootuser:rootpassword@postgres_db:5432/{db_name}"
        engine = create_engine(database_url)  # Create the engine
        return engine
    except Exception as e:
        print(f"Error establishing connection: {e}")
        return None


def upload_csv(filename, table_name, engine):
    df = pd.read_csv(filename)
    if(engine.connect()):
        try:
            df.to_sql(table_name, con=engine, if_exists='fail', index=False)
            print('csv file uploaded to the db as a table')
        except ValueError as e:
            print("Table already exists. Error:", e)
    else:
        print("Failed to connect to database")
        
   
