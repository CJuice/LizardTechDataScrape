"""
Walk the jobs directory for LizardTech Tool processing html files for values and assessing zip file size.
Walk the output jobs directory. Each job will contain an html file and likely a zip file. Convert the html file
to a dataframe and extract the values of interest. Assess the compressed size of the zip files. Output this information
to a single excel file with multiple sheets.

NOTE TO FUTURE DEVELOPERS: This was my first use of Pandas in a data processing script. The code may not designed
well since my focus was on using Pandas functionality and not overall architecture. The script Could be revised
at a later date when it can be cleaned up.

datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")
Author: CJuice
Date Created: 20190306
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
    import urllib.parse as urlpar

    # VARIABLES
    # jobs_folder = r'export_dir2'
    # output_folder = r'GrabLizardTechOutputLogInfo'
    jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
    output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo'  # Production

    # FUNCTIONS
    def convert_start_date_time_to_datetime(value):
        """

        :param value:
        :return:
        """
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
        """

        :param df:
        :return:
        """
        #   count email occurrences
        email_counts_series = df["Email"].value_counts(normalize=False, sort=True, ascending=True, dropna=False)
        email_counts_df = email_counts_series.to_frame()
        email_counts_df.reset_index(inplace=True)
        email_counts_df.rename(columns={"Email": "Count", "index": "Email"}, inplace=True)
        email_counts_df.sort_values(by=["Count"], ascending=False, inplace=True)
        return email_counts_df

    def create_output_file_path(extension: str) -> str:
        """

        :param extension:
        :return:
        """
        date_string = f"{datetime.datetime.today().year}-{datetime.datetime.today().month}-{datetime.datetime.today().day}"
        return os.path.join(output_folder, f"LizardTechAnalysis_{date_string}.{extension}")

    def determine_unique_email_extensions(df: pd.DataFrame) -> pd.DataFrame:
        """

        :param df:
        :return:
        """
        # count email types based on top-level domain values
        # TODO: Will need to build in protection for addresses that are bogus and don't have '@' or '.'
        email_parts_series = df["Email"].apply(func=lambda x: x.split("@"))
        domain_series = email_parts_series.apply(func=lambda x: x[-1])
        top_level_domain_series = domain_series.apply(func=lambda x: x.split(".")[-1])
        unique_values = top_level_domain_series.value_counts()
        unique_values_df = unique_values.to_frame().reset_index()
        unique_values_df.rename(columns={"index": "TopLevelDomain", "Email": "Count"}, inplace=True)
        return unique_values_df

    def extract_email_series_from_messages(df: pd.DataFrame) -> pd.Series:
        """

        :param df:
        :return:
        """
        df_no_na = df.dropna()
        df_no_na = df_no_na[df_no_na["Message"].str.contains("@")]
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

    def extract_issuing_url_series(df: pd.DataFrame) -> pd.Series:
        """
        Examine the Messages column, identifying the 'Issuing URL: ' records, extract url, return a Series
        :param df:
        :return:
        """
        df_no_na = df.dropna()
        messages_df = df_no_na[["Message"]]
        messages_df = messages_df[messages_df["Message"].str.startswith("Issuing URL: ")]
        try:
            url_series = messages_df["Message"].apply(func=lambda x: x[13:])
        except ValueError as ve:
            return pd.Series()
        except IndexError as id:
            return pd.Series()
        else:
            return url_series

    def extract_job_start_date_time_line(file_path: str) -> str:
        """

        :param file_path:
        :return:
        """
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

    def extract_query_string_dicts(series: pd.Series) -> pd.Series:
        """
        Parse the query string parameters and values from a Series of url values and return results as a dictionary
        :param series:
        :return:
        """
        query_string_dicts_series = (series.apply(func=lambda x: urlpar.parse_qs(qs=urlpar.urlparse(x).query)))
        # .reset_index(drop=True))
        return query_string_dicts_series

    def process_level_summary_by_job(df: pd.DataFrame) -> list:
        """

        :param df:
        :return:
        """
        level_summary_ls = []
        level_groupeddf = df.groupby([df.index])

        for name, group in level_groupeddf:
            level_df = group["Level"].value_counts().to_frame()
            level_df["JOB_ID"] = name
            level_summary_ls.append(level_df)

        return level_summary_ls

    def setup_initial_dataframe(file_path: str) -> pd.DataFrame:
        """
        Process html file, containing a single table, into a dataframe with the same columns as the table
        :param file_path:
        :return:
        """
        # Use pandas to create list of dataframes from tables in html. Should only be one per file so get zero index.
        df = pd.read_html(io=file_path)[0]

        # For unknown reason, true table headers are in row 0. Grab them, use to rename columns, drop first row
        #   Columns are given a 0 through 4 index instead of the header value. Need dict to rename them.
        column_names_series = df.iloc[0]
        column_rename_dict = dict(zip(list(range(0, len(column_names_series))), column_names_series))
        df.rename(columns=column_rename_dict, inplace=True)
        df.drop([0], axis=0, inplace=True)  # drop first row, which is the headers
        return df

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
                start_dtobj_UTC = convert_start_date_time_to_datetime(value=start_dt_string)
                # from_zone = dateutil.tz.tzutc()
                to_zone = dateutil.tz.tzlocal()
                start_dtobj_UTC.replace(tzinfo=to_zone)  # Not sure how will be affected by time changes on my puter

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
    #   JOB VALUES AS DATAFRAME
    #   Need to make single master html content and zip content dataframes from list of individual file dataframes
    try:
        master_html_values_df = pd.DataFrame(pd.concat(objs=master_html_df_list))
        master_html_values_df.set_index(keys="JOB_ID", drop=True, inplace=True)
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
    #   LEVEL SUMMARY (INFO, ERROR)
    level_summary_list = process_level_summary_by_job(df=master_html_values_df)
    master_level_df = pd.DataFrame(pd.concat(objs=level_summary_list))
    master_level_df.reset_index(drop=False, inplace=True)
    master_level_df.rename(columns={"index": "Level", "Level": "Count"}, inplace=True)
    level_groupby_df = master_level_df.groupby(by=["JOB_ID", "Level"]).mean()

    # ___________________________
    #   EMAIL PROCESSING
    #   isolate the html file Message values that contain an '@'
    emails_df = (extract_email_series_from_messages(df=master_html_values_df)
                 .to_frame(name="Email")
                 .reset_index())

    #   process email occurrences
    email_counts_df = count_email_occurrences(df=emails_df)

    #   process emails for the unique extensions (gov, com, edu, etc) that occur
    unique_email_extensions_df = determine_unique_email_extensions(df=email_counts_df)

    # ___________________________
    #   ISSUING URL PROCESSING
    #   Issuing url query string value extraction
    issuing_url_series = extract_issuing_url_series(df=master_html_values_df)  # This series contains a job id index
    issuing_url_query_string_dicts_series = extract_query_string_dicts(issuing_url_series)

    # QUERY PARAMETER EXAMINATION - MULTIPLE OUTPUTS GENERATED
    # Iterate over the query parameters in the issuing url's in the html logs, simmer down to unique occurrences
    #   by job, then get the overall number of times (number of unique jobs) that a value was used/requested by a user

    # NOTE: Changing the order of the dictionary values will change the order of the excel tabs
    query_parameter_explanation = {"cat": "Catalog",
                                   "thinningFactor": "Thinning Factor",
                                   "srs": "Spatial Reference System",
                                   "class": "Classifications",
                                   "res": "Resolution",
                                   "dt": "Data Type",
                                   "oif": "Output Format",
                                   "bounds": "Exporting Extent",
                                   "item": "Unknown Meaning",
                                   }

    query_parameter_values_df_dict = {}

    # Create a dataframe for each query parameter and store in dictionary with explanation term as key
    for key, value in query_parameter_explanation.items():
        try:
            query_param_df = issuing_url_query_string_dicts_series.apply(func=lambda x: x.get(key, np.NaN))
            query_parameter_values_df_dict[value] = query_param_df
        except KeyError:
            print(f"Key Error: {key} not found")
            pass
        else:
            pass

    # Create job id grouped, unique values dataframes for all query parameters.
    #   Must get unique occurrence for each job, otherwise counts influenced by quantity of issuing url requests
    # all_jobs_df = master_html_values_df.index.unique().to_frame(index=False)  # TURNED OFF
    query_param_unique_dfs_dict = {}
    for key, value in query_parameter_values_df_dict.items():
        query_param_values_df = value.to_frame(name=key).dropna(axis=0, how='any', inplace=False)
        query_param_values_df[key] = query_param_values_df[key].apply(lambda x: x[0])
        query_param_values_df.rename(columns={"Message": key}, inplace=True)
        query_param_values_df[key] = query_param_values_df[key].apply(lambda x: tuple([x]))  # pd.unique() won't work on lists, unhashable, cast to tuple
        unique_gb = query_param_values_df.groupby("JOB_ID")
        unique_results_df = unique_gb[key].unique().to_frame()

        # all_jobs_df = all_jobs_df.join(other=unique_results_df, on="JOB_ID", how="left")  # TURNED OFF

        list_of_unique_tuples = unique_results_df[key].tolist()
        unique_tuples_list = []
        for item in list_of_unique_tuples:
            # NOTE: Items are np.ndarray. Encountered issues in value_counts() with ndarray's. Needed them as lists.
            #   Otherwise they don't always compare equally, it seems, and values that are visually identical are not
            #   counted in the same bucket but just repeat as single counts. Converting to lists solved the problem.
            unique_tuples_list.extend(np.ndarray.tolist(item))

        uniques_value_counts_series = pd.Series(data=unique_tuples_list).value_counts()
        uniques_df = uniques_value_counts_series.to_frame().rename(columns={0: "Job Count"}, inplace=False)
        uniques_df.index.rename(name=key, inplace=True)
        query_param_unique_dfs_dict[key] = uniques_df

        # Final conversion of values to strings so that print out to excel doesn't show tuple container symbols
        query_param_unique_dfs_dict[key].reset_index(inplace=True)
        query_param_unique_dfs_dict[key][key] = query_param_unique_dfs_dict[key][key].apply(lambda x: x[0])

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
        master_zip_stats_df.to_excel(excel_writer=xlsx_writer,
                                     sheet_name="Job .zip Size Summary",
                                     na_rep=np.NaN,
                                     header=True,
                                     index=False)
        for key, value in query_param_unique_dfs_dict.items():
            value.to_excel(excel_writer=xlsx_writer,
                           sheet_name=f"QP - {key}",
                           na_rep=np.NaN,
                           header=True,
                           index=False)

        # TURNED OFF
        # all_jobs_df.to_excel(excel_writer=xlsx_writer,
        #                      sheet_name="Unique Query Parameters per Job",
        #                      na_rep=str(np.NaN),
        #                      header=True,
        #                      index=False)

        print("Process Complete")


if __name__ == "__main__":
    main()
