"""
Walk the jobs directory for LizardTech Tool processing html files for values and assessing zip file size.
Walk the output jobs directory. Each job will contain an html file and likely a zip file. Convert the html file
to a dataframe and extract the values of interest. Assess the compressed size of the zip files. Output this information
to a single excel file with multiple sheets.

datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")
Author: CJuice
Date Created: 20190306
Revisions:
20190530, CJuice: when no zip files found, there was no dataframe to write. When no zips, now a
    basically blank dataframe is created to avoid raising exception.
20190711, CJuice: added step to check column names and make sure all html df's had same columns. Add new if missing one
    and set to default value. Then when concat all columns identical.
20190809, CJuice: changed output file date string to have leading zeros for single digit months and days so that
    file name sort properly. Added functionality generating new excel sheet containing job id, the spatial reference
    system used, and the export extent of each job so that the extents can be mapped.
20190821, CJuice: Added functionality to remove duplicate issuing url's for jobs. This was causing duplicate values
    like export extents. Changed job_id to include job time value in milliseconds to help avoid issues with
    multiple jobs that happen to have the same name. The timestamp will help differentiate these jobs. Changed order
    of sheets in output file so mappable extent is more quickly accessed.

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
    # jobs_folder = r'export_dir_lidar'   # TESTING
    jobs_folder = r'export_dir2_lidar'   # TESTING
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
        date_string = datetime.datetime.now().strftime("%Y-%m-%d")
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
        Parse the query string parameters and values from a Series of url values and return results as a series. The
        index of the series is the Job ID and the values are a dict of query string parameter keys and list of values as
        values in the dict.
        :param issuing_url_ser: series of query string values parsed from issuing urls
        :return: series of query parameters and values
        """
        query_string_dicts_series = issuing_url_ser.apply(func=lambda x: urlpar.parse_qs(qs=urlpar.urlparse(x).query))
        return query_string_dicts_series

    def isolate_value_in_list_or_replace_null(attr_val):
        """
        Detect numpy NaN values (type float when inspect) and replace with custom value.
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
            job_id = os.path.basename(os.path.dirname(full_file_path))  # In Prod, the folder name is the job id
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

                # Need to store the date of the job for use in visualizations
                html_df["Job_Date"] = start_dtobj_utc

                # Need to add a unique job id field to be able to group message content and also relate dataframes
                #   Set this job id as the index and store the dataframe for this html file in the master list
                composite_job_id = f"{job_id.replace(' ','_')}_{start_dtobj_utc.timestamp()}"  # Trying new job id format to avoid issues with situation where two different jobs are named same exact name

                # Was seeing unexpected empty names issue somehow, so introduced handling
                # if composite_job_id.strip() == "" or len(composite_job_id) == 0:
                #     composite_job_id = f"UnknownJobName_{start_dtobj_utc.timestamp()}"

                html_df["JOB_ID"] = composite_job_id

                # Some columns appear to be missing the "JOB_ID" column and have a column named "Unnamed: 5". This
                #   causes an issue during the concatenation step. For those with Unnamed: 5, add a JOB_ID and set NaN
                try:
                    html_df.drop(columns=["Unnamed: 5"], inplace=True)
                except KeyError as ke:
                    # If 'Unnamed: 5' column doesn't exist then no problem, keep moving.
                    pass

                master_html_df_list.append(html_df)

            elif file_ext == ".zip":

                # What is the compressed job size of the .zip file, if .zip is present. Create dataframe for this .zip
                #   file and store in the master list
                byte_size = os.path.getsize(full_file_path) / 1000
                data = {"Name": [job_id], "ZIP Size KB": [byte_size]}  # This job_id won't fully match composite_job_id
                df = pd.DataFrame(data=data, dtype=str)

                master_zip_df_list.append(df)

            else:
                # Not interested in any other file types, if they happen to exist
                continue

    # ___________________________
    #   JOB VALUES AS DATAFRAME
    #   Need to make single master html content and zip content dataframes from list of individual file dataframes
    try:
        master_html_values_df = pd.DataFrame(pd.concat(objs=master_html_df_list))
        # print(master_html_values_df.info())
        # print(master_html_values_df["JOB_ID"].head(40))
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
        master_zip_stats_df = pd.DataFrame(data={"No Zip Files Found": [0]})

    job_to_date_df = master_html_values_df[["Job_Date"]].drop_duplicates()

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
    issue_url_size_with_duplicates = issuing_url_series.size

    # Need to remove duplicate issuing urls before continuing.
    # NOTE: There is a getdem and a getcloud url that have identical query parameters so more duplicate extent removal
    #   occurs later in this process.
    issuing_url_df = issuing_url_series.to_frame()
    issuing_url_df.drop_duplicates(inplace=True)
    issuing_url_series_no_dup = issuing_url_df["Message"]
    issue_url_size_without_duplicates = issuing_url_series_no_dup.size
    print(f"{issue_url_size_with_duplicates - issue_url_size_without_duplicates} Issuing URLs Duplicates Removed ")
    issuing_url_query_string_dicts_series = extract_query_string_dicts(issuing_url_ser=issuing_url_series_no_dup)

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
    for query_param_key, parameter_name in query_parameter_explanation.items():
        try:
            query_param_ser = issuing_url_query_string_dicts_series.apply(func=lambda x: x.get(query_param_key, np.NaN))
            query_parameter_values_df_dict[parameter_name] = query_param_ser
        except KeyError:
            print(f"Key Error: {query_param_key} not found")
            pass
        else:

            pass

    # Create job id grouped, unique values dataframes for all query parameters.
    #   Must get unique occurrence for each job, otherwise counts influenced by quantity of issuing url requests
    # all_jobs_df = master_html_values_df.index.unique().to_frame(index=False)  # TURNED OFF

    unique_results_by_job_dict = {}
    query_param_unique_dfs_dict = {}
    for query_param_key, value_ser in query_parameter_values_df_dict.items():

        # need dataframe from each series
        query_param_values_df = value_ser.to_frame(name=query_param_key)

        # need to process values and substitute meaningful empty string or extract value in the value list
        query_param_values_df[query_param_key] = query_param_values_df[query_param_key].apply(isolate_value_in_list_or_replace_null)

        # rename columns to meaningful/accurate term
        query_param_values_df.rename(columns={"Message": query_param_key}, inplace=True)

        # need values list as tuple for .unique() to work...  pd.unique() won't work on lists, unhashable, cast to tuple
        query_param_values_df[query_param_key] = query_param_values_df[query_param_key].apply(lambda x: tuple([x]))

        # need data grouped by job id so performing for each job
        unique_gb = query_param_values_df.groupby("JOB_ID")

        # need the unique values and convert to dataframe
        unique_results_df = unique_gb[query_param_key].unique().to_frame()

        # need to save dataframes for later use
        unique_results_by_job_dict[query_param_key] = unique_results_df

        # all_jobs_df = all_jobs_df.join(other=unique_results_df, on="JOB_ID", how="left")  # TURNED OFF

        list_of_unique_tuples = unique_results_df[query_param_key].tolist()

        unique_tuples_list = []
        for item in list_of_unique_tuples:
            # NOTE: Items are np.ndarray. Encountered issues in value_counts() with ndarray's. Needed them as lists.
            #   Otherwise they don't always compare equally, it seems, and values that are visually identical are not
            #   counted in the same bucket but just repeat as single counts. Converting to lists solved the problem.
            # NOTE: Encountered issue here. Converting ndarray to a list wiped the few unique catalog items that
            #   contained more than one catalog name. Had to adjust by putting each item into a single tuple
            #   containing a comma sep string

            # generic conversion
            # converted = np.ndarray.tolist(item)

            # special situation conversion
            if query_param_key == "Catalog" and 1 < len(item):
                blank_string = ""
                for val in item:
                    val = val[0]
                    if blank_string == "":
                        blank_string = val
                    else:
                        blank_string += f", {val}"
                converted = [(blank_string,)]   # Using extend on tuple adds the string value to the list, not a tuple
            else:
                # generic conversion
                converted = np.ndarray.tolist(item)

            # master list of values, regardless of generic or special
            unique_tuples_list.extend(converted)

        # need the unique tuples list as a series to use value_counts function
        uniques_value_counts_series = pd.Series(data=unique_tuples_list).value_counts()

        # need to convert to a dataframe, rename so meaningful
        uniques_df = uniques_value_counts_series.to_frame().rename(columns={0: "Job Count"}, inplace=False)
        uniques_df.index.rename(name=query_param_key, inplace=True)

        query_param_unique_dfs_dict[query_param_key] = uniques_df

        # Final conversion of values to strings so that print out to excel doesn't show tuple container symbols
        query_param_unique_dfs_dict[query_param_key].reset_index(inplace=True)

        # Need the dataframe value from the dict for the key of interest, then get the column of interest, then extract
        #   from the tuple the value so isn't in tuple format in output
        # Note: first query_param_key call gets dictionary key and associated dataframe value. Second query_parm_key
        #   call gets the column of interest from the dataframe. So, it is accessing a series.
        query_param_unique_dfs_dict[query_param_key][query_param_key] = query_param_unique_dfs_dict[query_param_key][query_param_key].apply(lambda x: x[0])

    # MAPPABLE EXPORT EXTENTS
    # Need dataframe containing spatial reference sys, export extent coords, and date for mapping lidar download
    # Need the spatial ref sys series
    srs_ser = query_parameter_values_df_dict["Spatial Reference System"]
    srs_ser.name = "Spatial Ref Sys"

    # Need the exporting extent series
    export_extent_ser = query_parameter_values_df_dict["Exporting Extent"]
    export_extent_ser.name = "Export Extent"

    # Need the srs and extents together to know how extent coords plot
    mappable_extent_df = pd.concat([srs_ser, export_extent_ser], axis=1)
    mappable_extent_df["Spatial Ref Sys"] = mappable_extent_df["Spatial Ref Sys"].apply(lambda x: x[0]) # extract string

    # Need to remove duplicate extents for jobs but first have to convert extent lists to tuples so hashable
    mappable_extents_with_duplicates = mappable_extent_df.size
    mappable_extent_df["Export Extent"] = mappable_extent_df["Export Extent"].apply(lambda x: tuple(x))
    mappable_extent_df.drop_duplicates(inplace=True)
    mappable_extents_without_duplicates = mappable_extent_df.size
    print(f"{mappable_extents_with_duplicates - mappable_extents_without_duplicates} Duplicate Mappable Extents Removed")
    # TODO: May revisit the tuple format if causes issues with use of the extents in mapping

    # Need the job date so can map extents with a time component. join job date table to mappable extents
    mappable_extent_df = mappable_extent_df.join(other=job_to_date_df, how="left")

    # ___________________________
    # SPATIAL EXAMINATION OF EXPORT EXTENT
    # TODO: Develop functionality once manually work out best process, destination for viz, etc.
    # ___________________________

    # ___________________________
    # DATE RANGE EVALUATION
    date_range_df = pd.DataFrame(data=[[np.min(date_range_list), np.max(date_range_list)]],
                                 columns=["MIN JOB DATE", "MAX JOB DATE"],
                                 dtype=str)

    # ___________________________
    #   OUTPUT THE EVALUATIONS
    #   Output various final contents to a unique sheet in excel file
    output_file_path = create_output_file_path(extension="xlsx")
    with pd.ExcelWriter(output_file_path) as xlsx_writer:
        date_range_df.to_excel(excel_writer=xlsx_writer,
                               sheet_name="Date Range of Jobs in Analysis",
                               na_rep=np.NaN,
                               header=True,
                               index=False)
        mappable_extent_df.to_excel(excel_writer=xlsx_writer,
                                    sheet_name="Mappable Extents",
                                    na_rep=np.NaN,
                                    header=True,
                                    index=True)
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
        for query_param_key, value_ser in query_param_unique_dfs_dict.items():
            value_ser.to_excel(excel_writer=xlsx_writer,
                               sheet_name=f"QP - {query_param_key}",
                               na_rep=np.NaN,
                               header=True,
                               index=False)


        # TURNED OFF
        # all_jobs_df.to_excel(excel_writer=xlsx_writer,
        #                      sheet_name="Unique Query Parameters per Job",
        #                      na_rep=str(np.NaN),
        #                      header=True,
        #                      index=False)

    print(f"Process Complete. See output file {output_file_path}")


if __name__ == "__main__":
    main()
