import datetime
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


# test_file = r"export_dir\ExampleJobOutput.html"
jobs_folder = r'export_dir'
output_folder = r'GrabLizardTechOutputLogInfo'
# jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
# output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo'   # Production
output_file_name = f"X_emailOutput_{datetime.date.today()}.csv"
output_file_path = os.path.join(output_folder, output_file_name)


# TESTING
master_email_series_list = []

# Walk jobs folder looking for html files
for dirname, dirs, files in os.walk(jobs_folder):

    # Iterate over files
    for file in files:
        file = file.strip()

        # Isolate html files
        if file.endswith('.html'):
            full_file_path = os.path.join(dirname, file)
            messages_list = []

            # Attempt to open file and convert html contents to soup
            try:
                with open(full_file_path, "r") as job_html_file_handler:
                    soup = BeautifulSoup(markup=job_html_file_handler.read(), features="html.parser")
            except FileNotFoundError as fnfe:
                print(f"File not found: {full_file_path} {fnfe}")
                continue

            # Find all table data elements, get the title for each, check for those == "Message" and accumulate text
            for td in soup.find_all('td'):
                try:
                    title_attr = td.attrs["title"]
                except KeyError:
                    # No title exists so move on
                    continue
                else:
                    if title_attr == "Message":
                        messages_list.append(td.text)

            # Build a pandas dataframe of messages
            msg_df = pd.DataFrame(messages_list, columns=["Messages"])

            # Use a boolean mask of strings containing @ symbol and apply to messages dataframe
            msg_df = msg_df[msg_df["Messages"].str.contains("@")]

            # Apply a function to the messages column and save results to dataframe
            #   NOTE: re.findall returns a list and I only observed one email per list in my test data
            email_series = msg_df["Messages"].apply(func=lambda x: re.findall(r'[\w.-]+@[\w.-]+', x)[0].lower())

            # Accumulate each files email dataframe
            master_email_series_list.append(email_series)

master_df = pd.DataFrame(pd.concat(objs=master_email_series_list, axis=0, join="outer", ignore_index=True))
master_df.rename(columns={"Messages": "Email"}, inplace=True)
# master_df = pd.DataFrame(data=master_email_series_list, columns=["Email"]) # Doesn't work like this
unique_emails_list = master_df["Email"].unique()
unique_emails_list.sort()
# master_df.to_csv(path_or_buf=output_file_path, sep=",", na_rep=np.NaN)    # Works fine but is all emails
unique_emails_df = pd.DataFrame(unique_emails_list, columns=["Unique_Emails"])
unique_emails_df.to_csv(path_or_buf=output_file_path, sep=",", na_rep=np.NaN)
