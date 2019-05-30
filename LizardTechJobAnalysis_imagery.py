"""
Walk the jobs directory for LizardTech Tool processing html files for values and assessing zip file size.
Walk the output jobs directory. Each job will contain an html file and likely a zip file. Convert the html file
to a dataframe and extract the values of interest. Assess the compressed size of the zip files. Output this information
to a single excel file with multiple sheets.

datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")

Author: CJuice
Date Created: 20190311
Revisions: Forked from lidar version and adjusted to fit imagery. Deleted portions of original because the Imagery Logs
and the LIDAR Logs are not the same. Imagery lacks an Issuing URL so a lot of information is not available.

"""


def main():

    # IMPORTS
    import datetime
    import dateutil
    import dateutil.tz
    import numpy as np
    import os
    import pandas as pd
    import re

    # VARIABLES
    # jobs_folder = r'export_dir_imagery'   # TESTING
    # output_folder = r'GrabLizardTechOutputLogInfo_imagery'    # TESTING
    jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
    output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo\AnalysisProcessOutputs'  # Production

    # FUNCTIONS
    def convert_start_date_time_to_datetime(start_dt_str):
        """
        Parse string value for start date and time from html table to a datetime object and return object
        :param start_dt_str: string repre
        :return: datetime object
        """
        start_dt_str = start_dt_str.strip()
        if "EDT" in start_dt_str:
            replacement_result = start_dt_str.replace("EDT", "EST")
            result = dateutil.parser.parse(replacement_result) - datetime.timedelta(hours=1)
        else:
            result = dateutil.parser.parse(start_dt_str)
        return result

    def count_email_occurrences(emails_dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Process a dataframe of emails and return the count of occurrences for each unique email
        :param emails_dataframe: pandas dataframe containing emails from html log files
        :return: pandas dataframe of unique emails and count of occurrences among processed job log files
        """
        #   count email occurrences
        email_counts_series = emails_dataframe["Email"].value_counts(normalize=False, sort=True, ascending=True, dropna=False)
        email_counts_dframe = email_counts_series.to_frame()
        email_counts_dframe.reset_index(inplace=True)
        email_counts_dframe.rename(columns={"Email": "Count", "index": "Email"}, inplace=True)
        email_counts_dframe.sort_values(by=["Count"], ascending=False, inplace=True)
        return email_counts_dframe

    def create_output_file_path(extension: str) -> str:
        """
        Create the output file path string incorporating the date and return string
        :param extension: file extension to be appended on end of string
        :return: string to be used in naming output file
        """
        date_string = f"{datetime.datetime.today().year}-{datetime.datetime.today().month}-{datetime.datetime.today().day}"
        return os.path.join(output_folder, f"LizardTechAnalysis_imagery_{date_string}.{extension}")

    def determine_unique_email_extensions(unique_emails_df: pd.DataFrame) -> pd.DataFrame:
        """
        Count the number of occurrences of unique email extensions such as .gov or .com and return dataframe
        :param unique_emails_df: dataframe of unique emails from job logs
        :return: dataframe of unique extensions
        """
        # count email types based on top-level domain values
        # TODO: May need to build in protection for addresses that are bogus and don't have '@' or '.'
        email_parts_series = unique_emails_df["Email"].apply(func=lambda x: x.split("@"))
        domain_series = email_parts_series.apply(func=lambda x: x[-1])
        top_level_domain_series = domain_series.apply(func=lambda x: x.split(".")[-1])
        unique_values = top_level_domain_series.value_counts()
        unique_values_df = unique_values.to_frame().reset_index()
        unique_values_df.rename(columns={"index": "TopLevelDomain", "Email": "Count"}, inplace=True)
        return unique_values_df

    def extract_email_series_from_messages(html_table_df: pd.DataFrame) -> pd.Series:
        """
        Extract pandas series of email specific messages from the html job log table Messages column
        :param html_table_df: dataframe of entire html table contents
        :return: pandas series of email specific messages
        """
        df_no_na = html_table_df.dropna()
        df_no_na = df_no_na[(df_no_na["Message"].str.contains("email")) & (df_no_na["Message"].str.contains("@"))]    #

        try:
            # NOTE: re.findall returns a list and I only observed one email per list in my test data
            emails_series = df_no_na["Message"].apply(
                func=lambda x: (re.findall(pattern=r'[\w.-]+@[\w.-]+', string=x))[0].lower())
        except ValueError as ve:
            return pd.Series()
        except IndexError as id:
            return pd.Series()
        else:
            return emails_series

    def extract_job_start_date_time_line(file_path: str) -> str:
        """
        Open log file and extract the start time value from html content and return
        :param file_path: path to job log html file
        :return: string
        """
        key_phrase = "Log session start time"
        break_tag = "<br>"
        empty_string = ""
        with open(file_path, 'r') as handler:
            for line in handler:
                line = line.strip()
                if key_phrase in line:
                    line = line.replace(key_phrase, empty_string)
                    line = line.replace(break_tag, empty_string)
                    return line
                else:
                    continue
        return "NaN"

    def process_level_summary_by_job(html_table_df: pd.DataFrame) -> list:
        """
        Extract the Level information from the html table and summarize value counts for types present, returning list
        :param html_table_df: dataframe of entire html table contents
        :return: list of dataframes for each job
        """
        level_summary_ls = []
        level_grouped_df = html_table_df.groupby([html_table_df.index])
        for name, group in level_grouped_df:
            level_df = group["Level"].value_counts().to_frame()
            level_df["JOB_ID"] = name
            level_summary_ls.append(level_df)

        return level_summary_ls

    def setup_initial_dataframe(file_path: str) -> pd.DataFrame:
        """
        Process html file, containing a single table, into a dataframe with the same columns as the table
        :param file_path: path to the html job log file to be examined
        :return: dataframe of html table content
        """
        # Use pandas to create list of dataframes from tables in html. Should only be one per file so get zero index.
        job_log_file_html_table_contents_df = pd.read_html(io=file_path)[0]

        # For unknown reason, true table headers are in row 0. Grab them, use to rename columns, drop first row
        #   Columns are given a 0 through 4 index instead of the header value. Need dict to rename them.
        column_names_series = job_log_file_html_table_contents_df.iloc[0]
        column_rename_dict = dict(zip(list(range(0, len(column_names_series))), column_names_series))
        job_log_file_html_table_contents_df.rename(columns=column_rename_dict, inplace=True)
        job_log_file_html_table_contents_df.drop([0], axis=0, inplace=True)  # drop first row, which is the headers
        return job_log_file_html_table_contents_df

    # FUNCTIONALITY
    #   Need two lists for storing dataframe from each/every file being inspected. Lists will then become a master df
    master_html_df_list = []
    master_zip_df_list = []
    date_range_list = []

    #   Need to walk the jobs folder and operate on the files within
    for root, dirs, files in os.walk(jobs_folder):
        for file in files:
            full_file_path = os.path.join(root, file)
            file_name, file_ext = os.path.splitext(file)
            job_id = os.path.basename(os.path.dirname(full_file_path))
            time_file_last_modified = os.path.getmtime(full_file_path)
            date_range_list.append(datetime.datetime.fromtimestamp(time_file_last_modified))

            if file_ext == ".html":

                # Extract values such as job start date and time
                start_dt_string = extract_job_start_date_time_line(file_path=full_file_path)
                start_dtobj_utc = convert_start_date_time_to_datetime(start_dt_str=start_dt_string)
                # from_zone = dateutil.tz.tzutc()
                to_zone = dateutil.tz.tzlocal()
                start_dtobj_utc.replace(tzinfo=to_zone)  # Not sure how will be affected by time changes on my puter

                # Need master list of all dataframes, each containing the extracted html file values
                html_df = setup_initial_dataframe(file_path=full_file_path)

                # Need to add a unique job id field to be able to group message content and also relate dataframes
                #   Set this job id as the index and store the dataframe for this html file in the master list
                html_df["JOB_ID"] = job_id
                master_html_df_list.append(html_df)

            elif file_ext == ".zip":

                # What is the compressed job size of the .zip file, if .zip is present. Create dataframe for this .zip
                #   file and store in the master list
                byte_size = os.path.getsize(full_file_path) / 1000
                data = {"Name": [job_id], "ZIP Size KB": [byte_size]}
                master_zip_df_list.append(pd.DataFrame(data=data, dtype=str))

            else:
                # Not interested in any other file types, if they happen to exist
                continue

    # ___________________________
    #   ALL JOB VALUES AS DATAFRAME
    #   Need to make single master html content dataframe, and single zip file content dataframe from list of
    #       individual file dataframes
    try:
        master_html_values_df = pd.DataFrame(pd.concat(objs=master_html_df_list, sort=False))
        master_html_values_df.set_index(keys="JOB_ID", drop=True, inplace=True)
        master_html_values_df.drop(columns=["Unnamed: 5"], inplace=True)  # Strange column of garbage with few values
    except ValueError:
        print("No .html files found.")

    try:
        #   Original zip df set to dtype of str so numeric job names would not change if had leading zeros etc. But,
        #       need to cast the size values to float before writing to excel
        master_zip_stats_df = pd.DataFrame(pd.concat(objs=master_zip_df_list))
        master_zip_stats_df.reset_index(drop=True, inplace=True)
        master_zip_stats_df["ZIP Size KB"] = pd.to_numeric(master_zip_stats_df["ZIP Size KB"])
    except ValueError:
        print("No .zip files found.")

    # ___________________________
    #   Remove java error messages from Level column in master html dataframe
    master_html_values_df = master_html_values_df[(master_html_values_df["Level"] == "INFO") | (master_html_values_df["Level"] == "ERROR")]

    # ___________________________
    #   LEVEL SUMMARY (INFO, ERROR)
    level_summary_list = process_level_summary_by_job(html_table_df=master_html_values_df)
    master_level_df = pd.DataFrame(pd.concat(objs=level_summary_list))
    master_level_df.reset_index(drop=False, inplace=True)
    master_level_df.rename(columns={"index": "Level", "Level": "Count"}, inplace=True)
    master_level_df = master_level_df[(master_level_df["Level"] == "ERROR") | (master_level_df["Level"] == "INFO")]
    level_groupby_df = master_level_df.groupby(by=["JOB_ID", "Level"]).mean()

    # ___________________________
    #   EMAIL PROCESSING
    #   isolate the html file Message values that contain an '@'
    emails_df = (extract_email_series_from_messages(html_table_df=master_html_values_df)
                 .to_frame(name="Email")
                 .reset_index())

    #   process email occurrences
    email_counts_df = count_email_occurrences(emails_dataframe=emails_df)

    #   process emails for the unique extensions (gov, com, edu, etc) that occur
    unique_email_extensions_df = determine_unique_email_extensions(unique_emails_df=email_counts_df)

    # ___________________________
    #   ISSUING URL PROCESSING
    #   Issuing url query string value extraction
    # Doesn't exist in Imagery. See LIDAR script if need the code.

    # ___________________________
    # QUERY PARAMETER EXAMINATION - MULTIPLE OUTPUTS GENERATED
    # Iterate over the query parameters in the issuing url's in the html logs, simmer down to unique occurrences
    #   by job, then get the overall number of times (number of unique jobs) that a value was used/requested by a user
    # Doesn't exist in Imagery. See LIDAR script if need the code.

    # ___________________________
    # DATE RANGE EVALUATION
    date_range_df = pd.DataFrame(data=[[np.min(date_range_list), np.max(date_range_list)]],
                                 columns=["MIN JOB DATE", "MAX JOB DATE"],
                                 dtype=str)

    # ___________________________
    #   OUTPUT THE EVALUATIONS
    #   Output various final contents to a unique sheet in excel file
    with pd.ExcelWriter(create_output_file_path(extension="xlsx")) as xlsx_writer:
        date_range_df.to_excel(excel_writer=xlsx_writer,
                               sheet_name="Date Range of Jobs in Analysis",
                               na_rep=np.NaN,
                               header=True,
                               index=False)
        email_counts_df.to_excel(excel_writer=xlsx_writer,
                                 sheet_name="Unique Emails Summary",
                                 na_rep=np.NaN,
                                 header=True,
                                 index=False)
        unique_email_extensions_df.to_excel(excel_writer=xlsx_writer,
                                            sheet_name="Top-Level Domains Summary",
                                            na_rep=np.NaN,
                                            header=True,
                                            index=False)
        level_groupby_df.to_excel(excel_writer=xlsx_writer,
                                  sheet_name="Level Type Summary by Job",
                                  na_rep=np.NaN,
                                  header=True,
                                  index=True)

        print(f"Process Complete. See output file {create_output_file_path(extension='xlsx')}")


if __name__ == "__main__":
    main()
