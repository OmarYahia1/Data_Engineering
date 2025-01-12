import os
import pandas as pd
import numpy as np
from functions import rename_columns, new_index, dropcol, lowerstr, grade_mapping, labelEnc, lookup, handle_outliers, calculate_monthly_installment, calculate_top_categories, one_hot_encode, establish_connection, upload_csv
from sklearn.preprocessing import MinMaxScaler

raw_data_path = r'/m2_docker/data/fintech_data_30_52_3224.csv'
cleaned_data_path = r'/m2_docker/data/fintech_data_NETW_P2_52_3224_clean.csv'
lookup_table_path = r'/m2_docker/data/fintech_data_NETW_P2_52_3224_lookup_table.csv'


def main():
    if not os.path.exists(cleaned_data_path):
        con = establish_connection(db_name='my_database')
        print("Cleaned dataset not found. Preparing data...")
        d = pd.read_csv(raw_data_path)
        d = rename_columns(d)

        # Set 'customer_id' as the index of the DataFrame 'df'.
        df = new_index(d, 'customer_id')

        #Description column is dropped as it is the same as purpose feature.
        df = dropcol(df, 'description')
        #addr_state column is dropped as it the exact same as state feature. 
        df = dropcol(df, 'addr_state')
        df = lowerstr(df, 'type')
        df.type = df.type.replace('joint app', 'joint')
        df['letter_grade'] = df['grade'].apply(grade_mapping)
        original_df = df.copy()
        rate_grade = df.groupby('grade')['int_rate'].median()
        df.int_rate = df.int_rate.fillna(df.grade.map(rate_grade))
        df.annual_inc_joint = df.annual_inc_joint.fillna(0)
        df['income_range'] = pd.cut(df['annual_inc'], bins=[0, 50000, 100000, 150000, 200000, 250000, np.inf],
                                    labels=['Very-Low', 'Low', 'Mid-Low', 'Mid', 'Mid-High', 'High'])

        mode_annual_inc = df.groupby('income_range')['emp_title'].agg(
            lambda x: x.mode()[0] if not x.mode().empty else "Unknown")
        df['emp_title'] = df['emp_title'].fillna(df['income_range'].map(mode_annual_inc))
        mode_annual_inc2 = df.groupby('income_range')['emp_length'].agg(
            lambda x: x.mode()[0] if not x.mode().empty else "Unknown")
        df['emp_length'] = df['emp_length'].fillna(df['income_range'].map(mode_annual_inc2))
        df = dropcol(df, 'income_range')

        loan_status_order = {
            'Fully Paid': 6,
            'Current': 5,
            'In Grace Period': 4,
            'Late (16-30 days)': 3,
            'Late (31-120 days)': 2,
            'Charged Off': 1,
            'Default': 0
        }

        # rank the grades from 'A' to 'G' based on their performance, allowing for easier comparison.
        letter_grade_mapping = {
            'A': 6,
            'B': 5,
            'C': 4,
            'D': 3,
            'E': 2,
            'F': 1,
            'G': 0
        }
        # Map verification status to numerical values to capture the confidence level of the data.
        verification_status_mapping = {
            'Verified': 2,
            'Source Verified': 1,
            'Not Verified': 0
        }
        # Map home ownership status to numerical values to quantify the stability of the borrower's 
        # financial situation. This can be a significant feature for predicting loan repayment.
        home_ownership_mapping = {
            'OWN': 4,
            'MORTGAGE': 3,
            'RENT': 2,
            'ANY': 1,
            'NONE': 0
        }

        df = labelEnc(df, 'loan_status', loan_status_order)
        df = labelEnc(df, 'letter_grade', letter_grade_mapping)
        df = labelEnc(df, 'verification_status', verification_status_mapping)
        df = labelEnc(df, 'home_ownership', home_ownership_mapping)

        lookup_table_df = lookup(df, original_df)
        lookup_table_df.to_csv(r'/m2_docker/data/fintech_data_NETW_P2_52_3224_lookup_table.csv', index=False)

        df = handle_outliers(df, 'annual_inc')
        df = handle_outliers(df, 'avg_cur_bal')
        df = handle_outliers(df, 'tot_cur_bal')
        df = handle_outliers(df, 'int_rate')
        df = handle_outliers(df, 'loan_amount')
        df = handle_outliers(df, 'loan_amount')

        df['issue_date'] = pd.to_datetime(df['issue_date'], format="%d %B %Y", errors='coerce')
        df['month_number'] = df['issue_date'].dt.month
        df['salary_can_cover'] = df['annual_inc'] >= df['loan_amount']
        df['term_int'] = df.term.str.extract('(\d+)').astype(int)

        # Calculate the monthly installment
        df['installment_per_month'] = df.apply(
            lambda row: calculate_monthly_installment(row['loan_amount'], row['int_rate'], row['term_int']),
            axis=1
        )
        # Drop the temporary 'term_int' column after use, as it's no longer needed.
        df = dropcol(df, 'term_int')

        # List of categorical columns to be one-hot encoded
        categorical_columns = ['emp_title', 'emp_length',
                               'zip_code', 'state', 'term',
                               'type', 'purpose']

        # Loop through each categorical column and apply one-hot encoding
        for col in categorical_columns:
            top_x = calculate_top_categories(df, col, 3)
            one_hot_encode(df, col, top_x)

        min_max_scaler = MinMaxScaler()
        features_to_normalize = ['annual_inc', 'annual_inc_joint', 'avg_cur_bal', 'tot_cur_bal', 'loan_amount',
                                 'funded_amount', 'int_rate', 'installment_per_month']
        df[features_to_normalize] = min_max_scaler.fit_transform(df[features_to_normalize])

        import requests
        from bs4 import BeautifulSoup

        # URL of the page to scrape
        url = "https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971"

        # Send a request to the webpage
        response = requests.get(url)

        # Parse the content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table that contains the state data
        table = soup.find('table')

        # Extract rows from the table
        rows = table.find_all('tr')

        # Create a dictionary for mapping state abbreviations to full names
        state_mapping = {}

        # Loop through the rows and extract state abbreviation and full name
        for row in rows[1:]:  # [1:] skips the header row if present
            cols = row.find_all('td')  # Find all cells in the current row
            if len(cols) >= 2:  # Ensure there are at least two columns in the row
                full_name = cols[
                    0].text.strip()  # Get the full state name from the first column, and remove extra spaces
                abbr = cols[2].text.strip()  # Get the state abbreviation from the third column, and remove extra spaces
                state_mapping[abbr] = full_name  # Add the abbreviation and full name to the dictionary

        #mapping each state abbreviation in 'state' feature with its actual name.
        df['state_name'] = df.state.map(state_mapping)
        df.to_csv(r'/m2_docker/data/fintech_data_NETW_P2_52_3224_clean.csv', index=True)
        
        table_names = ['fintech_data_NETW_P2_52_3224_clean', 'fintech_data_NETW_P2_52_3224_lookup_table']
        for table_name in table_names:
            filename = f'/m2_docker/data/{table_name}.csv'
            upload_csv(filename=filename, table_name=table_name, engine=con)

    else:
        print("Cleaned dataset exists. Uploading to database...")
        # Upload to PostgreSQL
        con = establish_connection(db_name='my_database')
        table_names = ['fintech_data_NETW_P2_52_3224_clean', 'fintech_data_NETW_P2_52_3224_lookup_table']
        for table_name in table_names:
            filename = f'/m2_docker/data/{table_name}.csv'
            upload_csv(filename=filename, table_name=table_name, engine=con)


if __name__ == "__main__":
    main()
