"""
Walk the jobs directory for LizardTech Tool processing html files for values and assessing zip file size.
Walk the output jobs directory. Each job will contain an html file and likely a zip file. Convert the html file
to a dataframe and extract the values of interest. Assess the compressed size of the zip files. Output this information
to a single excel file with multiple sheets.

datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")
Author: CJuice
Date Created: 20190306
Revisions: 20190530, CJuice: when no zip files found, there was no dataframe to write. When no zips, now a
basically blank dataframe is created to avoid raising exception.

NOTE TO FUTURE DEVELOPERS: First use of Pandas in a data processing script. Code may not designed
well since focus was on using Pandas functionality, not overall architecture.
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
    jobs_folder = r'export_dir_lidar'   # TESTING
    output_folder = r'GrabLizardTechOutputLogInfo_lidar'    # TESTING
    # jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
    # output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo\AnalysisProcessOutputs'  # Production

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
        return os.path.join(output_folder, f"LizardTechAnalysis_lidar_{date_string}.{extension}")

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

    def extract_issuing_url_series(html_table_df: pd.DataFrame) -> pd.Series:
        """
        Examine the Messages column, identifying the 'Issuing URL: ' records, extract url, return a Series
        :param html_table_df: dataframe of entire html table contents
        :return: pandas series of Message content containing Issuing URL
        """
        df_no_na = html_table_df.dropna()
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

    def extract_query_string_dicts(issuing_url_ser: pd.Series) -> pd.Series:
        """
        Parse the query string parameters and values from a Series of url values and return results as a dictionary
        :param issuing_url_ser: series of query string values parsed from issuing urls
        :return: series of query parameters and values
        """
        query_string_dicts_series = (issuing_url_ser.apply(func=lambda x: urlpar.parse_qs(qs=urlpar.urlparse(x).query)))
        return query_string_dicts_series

    def isolate_value_in_list_or_replace_null(attr_val):
        """
        Detect numpy NaN values, type float when inspect, and replace with value.
        This was added so that null/empty query parameters can be included in counts, which is important for indicating
        how many jobs did not define the parameter.
        :param attr_val: string value to be substituted for null value
        :return: value inside of list or a string substitute for np.NaN value
        """

        if type(attr_val) is list:
            return attr_val[0]
        elif np.isnan(attr_val):
            return "DoIT Detected NULL"
        else:
            return "Unknown Value"

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

                # Some columns appear to be missing the "JOB_ID" column and have a column named "Unnamed: 5". This
                #   causes an issue during the concatenation step. For those with Unnamed: 5, add a JOB_ID and set NaN
                try:
                    html_df.drop(columns=["Unnamed: 5"], inplace=True)
                except KeyError as ke:
                    # If Unnamed: 5 columns doesn't exist then no problem, keep moving.
                    pass
                else:
                    html_df["JOB_ID"] = np.NaN

                master_html_df_list.append(html_df)

            elif file_ext == ".zip":

                # What is the compressed job size of the .zip file, if .zip is present. Create dataframe for this .zip
                #   file and store in the master list
                byte_size = os.path.getsize(full_file_path) / 1000
                data = {"Name": [job_id], "ZIP Size KB": [byte_size]}
                df = pd.DataFrame(data=data, dtype=str)

                master_zip_df_list.append(df)

            else:
                # Not interested in any other file types, if they happen to exist
                continue

    # ___________________________
    #   JOB VALUES AS DATAFRAME
    #   Need to make single master html content and zip content dataframes from list of individual file dataframes
    try:
        master_html_values_df = pd.DataFrame(pd.concat(objs=master_html_df_list))  #, sort=False
        master_html_values_df.set_index(keys="JOB_ID", drop=True, inplace=True)
    except ValueError:
        print("No .html files found.")
    print(master_html_values_df.info())

    try:
        #   Original zip df set to dtype of str so numeric job names would not change if had leading zeros etc. But,
        #       need to cast the size values to float before writing to excel
        master_zip_stats_df = pd.DataFrame(pd.concat(objs=master_zip_df_list))
        master_zip_stats_df.reset_index(drop=True, inplace=True)
        master_zip_stats_df["ZIP Size KB"] = pd.to_numeric(master_zip_stats_df["ZIP Size KB"])
    except ValueError:
        print("No .zip files found.")
        master_zip_stats_df = pd.DataFrame(data={"No Zip Files Found": [0]})

    # ___________________________
    #   LEVEL SUMMARY (INFO, ERROR)
    level_summary_list = process_level_summary_by_job(html_table_df=master_html_values_df)
    master_level_df = pd.DataFrame(pd.concat(objs=level_summary_list))
    master_level_df.reset_index(drop=False, inplace=True)
    master_level_df.rename(columns={"index": "Level", "Level": "Count"}, inplace=True)
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
    issuing_url_series = extract_issuing_url_series(html_table_df=master_html_values_df)  # This series contains a job id index
    issuing_url_query_string_dicts_series = extract_query_string_dicts(issuing_url_series)
    print(issuing_url_query_string_dicts_series)
    exit()
    # ___________________________
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

    unique_results_by_job_dict = {}
    query_param_unique_dfs_dict = {}
    for key, value in query_parameter_values_df_dict.items():
        query_param_values_df = value.to_frame(name=key)
        query_param_values_df[key] = query_param_values_df[key].apply(isolate_value_in_list_or_replace_null)
        # query_param_values_df[key] = query_param_values_df[key].apply(lambda x: x[0]) # Replaced by custom function
        query_param_values_df.rename(columns={"Message": key}, inplace=True)
        query_param_values_df[key] = query_param_values_df[key].apply(lambda x: tuple([x]))  # pd.unique() won't work on lists, unhashable, cast to tuple
        unique_gb = query_param_values_df.groupby("JOB_ID")
        unique_results_df = unique_gb[key].unique().to_frame()
        unique_results_by_job_dict[key] = unique_results_df

        # all_jobs_df = all_jobs_df.join(other=unique_results_df, on="JOB_ID", how="left")  # TURNED OFF

        list_of_unique_tuples = unique_results_df[key].tolist()
        unique_tuples_list = []
        for item in list_of_unique_tuples:
            # NOTE: Items are np.ndarray. Encountered issues in value_counts() with ndarray's. Needed them as lists.
            #   Otherwise they don't always compare equally, it seems, and values that are visually identical are not
            #   counted in the same bucket but just repeat as single counts. Converting to lists solved the problem.
            # NOTE: Encountered issue here. Converting ndarray to a list wiped the few unique catalog items that
            #   contained more than one catalog name. Had to adjust by putting each item into a single tuple
            #   containing a comma sep string
            converted = np.ndarray.tolist(item)
            if key == "Catalog" and len(item) > 1:
                blank_string = ""
                for val in item:
                    val = val[0]
                    if blank_string == "":
                        blank_string = val
                    else:
                        blank_string += f", {val}"
                converted = [(blank_string,)]   # Using extend on tuple adds the string value to the list, not a tuple
            unique_tuples_list.extend(converted)

        uniques_value_counts_series = pd.Series(data=unique_tuples_list).value_counts()
        uniques_df = uniques_value_counts_series.to_frame().rename(columns={0: "Job Count"}, inplace=False)
        uniques_df.index.rename(name=key, inplace=True)
        query_param_unique_dfs_dict[key] = uniques_df

        # Final conversion of values to strings so that print out to excel doesn't show tuple container symbols
        query_param_unique_dfs_dict[key].reset_index(inplace=True)
        query_param_unique_dfs_dict[key][key] = query_param_unique_dfs_dict[key][key].apply(lambda x: x[0])

    # ___________________________
    # SPATIAL EXAMINATION OF EXPORT EXTENT
    # TODO: Stopped development on this section. Continue when time available. CJuice 20190306
    def process_raw_extent_value(val):
        """

        :param val:
        :return:
        """
        val_inner_tuple = val[0]
        inner_val_as_list = list(val_inner_tuple)
        split_inner_val_list = inner_val_as_list[0].split(",")
        if len(split_inner_val_list) == 5:
            split_inner_val_list.pop(5)
            split_inner_val_list.pop(2)
            return split_inner_val_list
        else:
            try:
                split_inner_val_list.remove("-Infinity")
                split_inner_val_list.remove("Infinity")
            except Exception as e:
                return split_inner_val_list
            else:
                return split_inner_val_list

    def process_raw_epsg_values(val):
        """

        :param val:
        :return:
        """
        return str(val[0][0].split(":")[1])

    exporting_extent_df = unique_results_by_job_dict["Exporting Extent"]["Exporting Extent"].apply(process_raw_extent_value).to_frame()
    epsg_df = unique_results_by_job_dict["Spatial Reference System"]["Spatial Reference System"].apply(process_raw_epsg_values).to_frame()
    spatial_ready_df = exporting_extent_df.join(other=epsg_df, on="JOB_ID", how="left")
    print(spatial_ready_df)
    # Create a polygon from each bounding extent in the epsg that is meaningful for the values

    # Re project the polygons to a common datum

    # Add these to a feature class with date.

    exit()

    pass    # For above and below pycharm comments to be separate and foldable

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

        print(f"Process Complete. See output file {create_output_file_path(extension='xlsx')}")


if __name__ == "__main__":
    main()
