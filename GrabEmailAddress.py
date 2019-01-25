"""

Notes:
    datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")
"""


def main():
    import datetime
    import dateutil
    import dateutil.tz
    import os
    import re
    from bs4 import BeautifulSoup
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import xlrd
    import html5lib
    import lxml



    # test_file = r"export_dir\ExampleJobOutput.html"
    jobs_folder = r'export_dir'
    output_folder = r'GrabLizardTechOutputLogInfo'
    # jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
    # output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo'   # Production
    # output_file_name = f"X_emailOutput_{datetime.date.today()}.csv"
    # output_file_path = os.path.join(output_folder, output_file_name)

    def convert_start_date_time_to_datetime(value):
        value = value.strip()
        if "EDT" in value:
            # print(f"EDT FLAG {file}")
            replacement_result = value.replace("EDT", "EST")
            result = dateutil.parser.parse(replacement_result) - datetime.timedelta(hours=1)
            # print(f"\t{value} has been converted to EST: {result}")
        else:
            result = dateutil.parser.parse(value)
        # print(f"\tRESULT: {result}")
        return result

    def count_email_occurrences(df: pd.DataFrame) -> pd.DataFrame:
        #   count email occurrences
        email_counts_series = df["Email"].value_counts(normalize=False, sort=True, ascending=True, dropna=False)
        email_counts_df = email_counts_series.to_frame()
        email_counts_df.reset_index(inplace=True)
        email_counts_df.rename(columns={"Email": "Count", "index": "Email"}, inplace=True)
        return email_counts_df

    def create_output_file_path(extension: str) -> str:
        date_string = f"{datetime.datetime.today().year}-{datetime.datetime.today().month}-{datetime.datetime.today().day}"
        return os.path.join(output_folder, f"X_LizardTechAnalysis_{date_string}.{extension}")

    def determine_unique_email_extensions(df: pd.DataFrame) -> pd.DataFrame:
        # count email types based on top-level domain values
        # TODO: Will need to build in protection for addresses that are bogus and don't have '@' or '.'
        email_parts_series = df["Email"].apply(func=lambda x: x.split("@"))
        domain_series = email_parts_series.apply(func=lambda x: x[-1])
        top_level_domain_series = domain_series.apply(func=lambda x: x.split(".")[-1])
        unique_values = top_level_domain_series.value_counts()
        unique_values_df = unique_values.to_frame().reset_index()
        unique_values_df.rename(columns={"index": "TopLevelDomain", "Email": "Count"}, inplace=True)
        return unique_values_df

    def extract_job_start_date_time_line(file_path: str) -> str:
        keyphrase = "Log session start time"
        break_tag = "<br>"
        empty_string = ""
        with open(file_path, 'r') as handler:
            for line in handler:
                line = line.strip()
                if keyphrase in line:
                    line = line.replace(keyphrase, empty_string)
                    line = line.replace(break_tag, empty_string)
                    return line
                else:
                    continue
        return "NaN"

    def extract_email_series_from_messages(df: pd.DataFrame)-> pd.Series:
        df_no_na = df.dropna()
        df_no_na = df_no_na[df_no_na["Message"].str.contains("@")]
        try:
            # NOTE: re.findall returns a list and I only observed one email per list in my test data
            emails_series = df_no_na["Message"].apply(func=lambda x: (re.findall(pattern=r'[\w.-]+@[\w.-]+', string=x))[0].lower())
        except ValueError as ve:
            return pd.Series()
        except IndexError as id:
            return pd.Series()
        else:
            return emails_series

    def setup_initial_dataframe(file_path: str) -> pd.DataFrame:
        # Use pandas to create list of dataframes from tables in html. Should only be 1 per file; Get 0 index.
        df = pd.read_html(io=file_path)[0]

        # For unknown reason, true table headers are in row 0. Grab them, use to rename columns, drop first row
        #   Columns are given a 0 through 4 index instead of the header value. Need dict to rename them.
        column_names_series = df.iloc[0]
        column_rename_dict = dict(zip(list(range(0, len(column_names_series))), column_names_series))
        df.rename(columns=column_rename_dict, inplace=True)
        df.drop([0], axis=0, inplace=True)  # drop first row
        return df

    master_email_series_list = []
    master_html_df_list = []

    # Walk jobs folder looking for html files
    for dirname, dirs, files in os.walk(jobs_folder):

        # Iterate over files
        for file in files:
            file = file.strip()
            # print(f"FILE: {file}")

            # Isolate html files
            if file.endswith('.html'):
                full_file_path = os.path.join(dirname, file)

                # Extract job start date and time
                start = extract_job_start_date_time_line(file_path=full_file_path)
                dt_start_UTC = convert_start_date_time_to_datetime(value=start)
                # from_zone = dateutil.tz.tzutc()
                to_zone = dateutil.tz.tzlocal()
                dt_start_UTC.replace(tzinfo=to_zone)    # Not sure how this will be affected by time changes on my puter

                html_df = setup_initial_dataframe(file_path=full_file_path)

                master_html_df_list.append(html_df)

    master_df = pd.DataFrame(pd.concat(objs=master_html_df_list)).reset_index()
    print(master_df.info())

    # EMAIL PROCESSING
    # isolate the Message values that contain '@'
    emails_df = extract_email_series_from_messages(df=master_df).to_frame(name="Email").reset_index()

    # process email occurrences
    email_counts_df = count_email_occurrences(df=emails_df)

    # process emails for unique extensions (gov, com, edu, etc)
    unique_email_extensions_df = determine_unique_email_extensions(df=email_counts_df)
    print(unique_email_extensions_df)
    # output various final contents to a unique sheet in excel file
    with pd.ExcelWriter(create_output_file_path(extension="xlsx")) as xlsx_writer:
        email_counts_df.to_excel(excel_writer=xlsx_writer, sheet_name="Unique Emails Summary", na_rep=np.NaN, header=True, index=False)
        unique_email_extensions_df.to_excel(excel_writer=xlsx_writer, sheet_name="Top-Level Domains Summary", na_rep=np.NaN, header=True, index=False)


if __name__ == "__main__":
    main()
