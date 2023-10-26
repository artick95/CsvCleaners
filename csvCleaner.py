import pandas as pd
import re

def clean_emails_with_name_prioritization():
    # Load the popular names CSV
    names_df = pd.read_csv("/Users/artick/Documents/Scripts/CsvCleaners/Popular Names by Country - USA Names.csvG")
    popular_names = names_df["Male Name"].str.lower().tolist() + names_df["Female Name"].str.lower().tolist()
    
    # Load CSV file with dtype=str
    youtubers_df = pd.read_csv("/Users/artick/Documents/Scripts/CsvCleaners/youtubersTest.csv", dtype=str)
    youtubers_df = youtubers_df.filter(regex='^(?!Unnamed)')
    youtubers_df['email'] = youtubers_df['email'].astype(str)
    
    email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    def is_valid_email(email):
        return bool(re.match(email_pattern, email))
    youtubers_df = youtubers_df[youtubers_df['email'].apply(is_valid_email)]
    grouped = youtubers_df.groupby('website')['email'].apply(list).reset_index()
    email_columns = grouped['email'].apply(pd.Series)
    
    # Check if email_columns is empty
    if email_columns.empty:
        print("No valid emails found.")
        return
    
    # Limit to max 6 email columns
    max_cols = 6
    if email_columns.shape[1] > max_cols:
        email_columns = email_columns.iloc[:, :max_cols]
        
    email_columns.columns = ['email' + str(i+1) for i in range(email_columns.shape[1])]
    
    def best_email(emails):
        if not emails:
            return None
        emails = sorted(emails, key=lambda email: (
            not any(name in email for name in popular_names),
            "info" in email,
            "contact" in email,
            "support" in email,
            "hello" in email,
            "admin" in email,
            email
        ))
        return emails[0]
    
    patterns_to_remove = [
        "domain.com", "sentry.io", "wixpress", "ingest", "example.com", ".edu",
        r"\.(jpg|jpeg|png|gif|bmp|webp|svg|mp4|avi|flv|mkv|mov)$", 
        r"@\d+\.\d+x"
    ]
    starting_patterns_to_remove = ["privacy", "terms"]
    def should_remove_email(email):
        if email is None or not isinstance(email, str):
            return False
        if any(re.search(pattern, email, re.IGNORECASE) for pattern in patterns_to_remove):
            return True
        if any(email.lower().startswith(pattern) for pattern in starting_patterns_to_remove):
            return True
        return False
    
    for col in email_columns.columns:
        email_columns[col] = email_columns[col].where(~email_columns[col].apply(should_remove_email), other=None)
    
    final_df = pd.concat([grouped['website'], email_columns], axis=1)
    final_df['emailBest'] = final_df.apply(lambda row: best_email(row.filter(like="email").dropna().tolist()), axis=1)
    
    # Reorder columns to make emailBest the second column
    cols = final_df.columns.tolist()
    cols.insert(1, cols.pop(cols.index('emailBest')))
    final_df = final_df[cols]
    
    final_df.to_csv("cleaned_output_file.csv", index=False)
    print("Email cleaning and prioritization complete. Output saved to cleaned_output_file.csv.")

clean_emails_with_name_prioritization()
