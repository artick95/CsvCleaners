import pandas as pd
import re

def clean_emails_from_csv():
    # Load CSV file with dtype=str
    youtubers_df = pd.read_csv("/Users/artick/Documents/Scripts/CsvCleaners/youtubersTest.csv", dtype=str)

    # Drop any unnamed columns (like "Unnamed: 2", "Unnamed: 3", etc.)
    youtubers_df = youtubers_df.filter(regex='^(?!Unnamed)')

    # Convert the email column to string type
    youtubers_df['email'] = youtubers_df['email'].astype(str)

    # Regular expression pattern for matching email format
    email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

    # Define a function to check if an email is valid
    def is_valid_email(email):
        return bool(re.match(email_pattern, email))

    # Filter out invalid emails using the is_valid_email function
    youtubers_df = youtubers_df[youtubers_df['email'].apply(is_valid_email)]

    # Group by website and get a list of emails associated with each website
    grouped = youtubers_df.groupby('website')['email'].apply(list).reset_index()

    # Convert the list of emails into separate columns
    email_columns = grouped['email'].apply(pd.Series)

    # Rename the columns to "email1", "email2", etc.
    email_columns.columns = [f'email{i+1}' for i in range(email_columns.shape[1])]

    # Concatenate the website and email columns
    final_df = pd.concat([grouped['website'], email_columns], axis=1)

    # Patterns for cleaning criteria
    patterns_to_remove = [
        "domain.com", "sentry.io", "wixpress", "ingest", "example.com", ".edu",
        r"\.(jpg|jpeg|png|gif|bmp|webp|svg|mp4|avi|flv|mkv|mov)$", 
        r"@\d+\.\d+x"
    ]
    starting_patterns_to_remove = ["privacy", "terms"]

    # Define a function to check if an email should be removed based on the patterns
    def should_remove_email(email):
        if email is None or not isinstance(email, str):
            return False
        if any(re.search(pattern, email, re.IGNORECASE) for pattern in patterns_to_remove):
            return True
        if any(email.lower().startswith(pattern) for pattern in starting_patterns_to_remove):
            return True
        return False

    # Apply the cleaning function to each email column and replace invalid emails with NaN
    for col in final_df.columns:
        if col.startswith("email"):
            final_df[col] = final_df[col].where(~final_df[col].apply(should_remove_email), other=None)

    # Save the cleaned dataframe to the specified output path
    final_df.to_csv("cleaned_output_file.csv", index=False)

# Call the function
clean_emails_from_csv()
