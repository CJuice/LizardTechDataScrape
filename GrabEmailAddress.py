"""

Notes:
    datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")
"""


def main():
    import datetime
    import os
    import re
    from bs4 import BeautifulSoup
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt

    # test_file = r"export_dir\ExampleJobOutput.html"
    jobs_folder = r'export_dir'
    output_folder = r'GrabLizardTechOutputLogInfo'
    # jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
    # output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo'   # Production
    # output_file_name = f"X_emailOutput_{datetime.date.today()}.csv"
    # output_file_path = os.path.join(output_folder, output_file_name)

    def create_output_file_path(file_content):
        return os.path.join(output_folder, f"X_{file_content}_{datetime.date.today()}.csv")

    def standardize_title_attr(title_attr):
        if "level" in title_attr.lower():
            return "Level"
        elif "category" in title_attr.lower():
            return "Category"
        elif "thread" in title_attr.lower():
            return "Thread"
        elif "message" in title_attr.lower():
            return "Message"
        else:
            return "NA"

    master_email_series_list = []

    # Walk jobs folder looking for html files
    for dirname, dirs, files in os.walk(jobs_folder):

        # Iterate over files
        for file in files:
            file = file.strip()

            # Isolate html files
            if file.endswith('.html'):
                full_file_path = os.path.join(dirname, file)
                titles_list = []
                # print(full_file_path)

                # Attempt to open file and convert html contents to soup
                try:
                    with open(full_file_path, "r") as job_html_file_handler:
                        soup = BeautifulSoup(markup=job_html_file_handler.read(), features="html.parser")
                except FileNotFoundError as fnfe:
                    print(f"File not found: {full_file_path} {fnfe}")
                    continue
                table = soup.find("table")
                # print(table.prettify())
                # exit()

                # Find all table header elements, get the text values for each
                header_list = []
                for th in table.find_all('th'):
                    try:
                        table_header = th.text
                    except KeyError:
                        # no headers to detect
                        continue
                    else:
                        header_list.append(table_header)
                # print(header_list)
                # exit()
                # Find all tr
                rows = table.find_all('tr')
                for row in rows:
                    table_datas = row.find_all('td')
                    # print(table_datas)
                    td_series = pd.Series(data=dict(zip(header_list, [td.text for td in table_datas])), index=header_list)
                    # print(td_series)
                    titles_list.append(td_series)

                titles_df = pd.DataFrame(titles_list).dropna()
                print(titles_df)

                exit()

                # TODO: Stopped refactoring here. Have full dataframe, now need to revise below and extract what I want.
                titles_df.set_index(keys=["Title"], drop=True, inplace=True)

                # CAPTURE EMAILS
                # Use a boolean mask of titles equal to Message
                messages_df = titles_df[(titles_df.index == "Message")]

                # Use boolean mask of Text containing @
                email_containing_messages_df = titles_df[titles_df["Text"].str.contains("@")]
                # if not email_containing_messages_df.empty:
                #     print(email_containing_messages_df.head())

                # Apply a function to the messages column and save results to dataframe
                #   NOTE: re.findall returns a list and I only observed one email per list in my test data
                email_series = (email_containing_messages_df["Text"]
                                .apply(func=lambda x: (re.findall(pattern=r'[\w.-]+@[\w.-]+', string=x)
                                                       )[0].lower()
                                       )
                                )

                # Accumulate each files email dataframe
                master_email_series_list.append(email_series)

    # EMAIL PROCESSING
    emails_df = pd.DataFrame(pd.concat(objs=master_email_series_list, axis=0, join="outer", ignore_index=True))
    emails_df.rename(columns={"Text": "Email"}, inplace=True)
    # master_df = pd.DataFrame(data=master_email_series_list, columns=["Email"]) # Doesn't work like this

    #   count email occurrences
    email_counts_series = emails_df["Email"].value_counts(normalize=False, sort=True, ascending=True, dropna=False)
    email_counts_df = email_counts_series.to_frame()
    email_counts_df.rename(columns={"Email": "Count"}, inplace=True)
    email_counts_df.index.name = "Email"
    email_counts_df.sort_index(axis=0, ascending=True, inplace=True, na_position="first")
    email_counts_df.to_csv(path_or_buf=create_output_file_path(file_content="EmailCounts"), sep=",", na_rep=np.NaN)
    # email_counts_series.hist()
    # plt.show()

    # NOTE: The counts list is also a unique emails list so only need to generate the counts df.
    #   unique emails
    # unique_emails_list = emails_df["Email"].unique()
    # unique_emails_list.sort()
    # # master_df.to_csv(path_or_buf=output_file_path, sep=",", na_rep=np.NaN)    # Works fine but is all emails
    # unique_emails_df = pd.DataFrame(unique_emails_list, columns=["Unique_Emails"])
    # unique_emails_df.to_csv(path_or_buf=create_output_file_path(file_content="EmailOutput"), sep=",", na_rep=np.NaN)

    #   count email types based on last three letters
    email_extensions_df = pd.DataFrame(email_counts_df.index.tolist(), columns=["Email"])
    email_extension_series = email_extensions_df["Email"].apply(func=lambda x: x.split("@")[1].split(".")[1])
    email_extension_series = email_extension_series.value_counts(normalize=False, sort=True, ascending=True, dropna=False)
    final_email_extensions_df = email_extension_series.to_frame()
    final_email_extensions_df.index.name = "Extension"
    final_email_extensions_df.to_csv(path_or_buf=create_output_file_path(file_content="EmailExtensionCounts"))


if __name__ == "__main__":
    main()
