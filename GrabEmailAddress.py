"""

Notes: This is my first use of Pandas etc in a data processing script. The code is not designed well because my focus
was on using Pandas functionality and not overall architecture. The script should be revised at a later date when
it can be cleaned up.
datetime.strptime("Nov 29 06:22:44 EST 2018", "%b %d %H:%M:%S EST %Y")
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
    # jobs_folder = r'export_dir'
    # output_folder = r'GrabLizardTechOutputLogInfo'
    jobs_folder = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # Production
    output_folder = r'D:\Scripts\GrabLizardTechOutputLogInfo'  # Production

    # FUNCTIONS
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
        email_counts_df.sort_values(by=["Count"], ascending=False, inplace=True)
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

    def extract_email_series_from_messages(df: pd.DataFrame) -> pd.Series:
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
        df_no_na = df.dropna()
        df_no_na = df_no_na[df_no_na["Message"].str.startswith("Issuing URL: ")]
        try:
            url_series = df_no_na["Message"].apply(func=lambda x: x[13:])
        except ValueError as ve:
            return pd.Series()
        except IndexError as id:
            return pd.Series()
        else:
            return url_series

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

    def extract_query_string_dicts(series: pd.Series) -> pd.Series:
        query_string_dicts_series = (series.apply(func=lambda x: urlpar.parse_qs(qs=urlpar.urlparse(x).query))
                                     .reset_index(drop=True))
        return query_string_dicts_series

    def inventory_catalog_job_request_count(df: pd.DataFrame) -> pd.DataFrame:

        # Goal is to extract the catalog names requested in each job, then sum the number of times
        #   each catalog is requested
        catalog_inventory_all_jobs = []
        job_name_groupeddf = df.groupby([df.index])

        for name, group in job_name_groupeddf:
            series_urls = extract_issuing_url_series(df=group)
            series_dicts = extract_query_string_dicts(series=series_urls)
            catalog_per_job_list = (series_dicts.apply(func=lambda x: x["cat"])
                                    .apply(func=lambda x: x[0])
                                    .unique()
                                    .tolist())
            catalog_inventory_all_jobs.extend(catalog_per_job_list)

        catalog_counts_df = (pd.Series(catalog_inventory_all_jobs)
                             .value_counts()
                             .to_frame())
        catalog_counts_df.reset_index(inplace=True)
        catalog_counts_df.rename(columns={"index": "Catalog Name", 0: "Job Count"}, inplace=True)
        return catalog_counts_df

    def process_level_summary_by_job(df: pd.DataFrame) -> list:
        level_summary_ls = []
        level_groupeddf = df.groupby([df.index])

        for name, group in level_groupeddf:
            level_df = group["Level"].value_counts().to_frame()
            level_df["JOB_ID"] = name
            level_summary_ls.append(level_df)

        return level_summary_ls

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

    # FUNCTIONALITY
    master_html_df_list = []
    master_zip_df_list = []

    # Walk jobs folder looking for html files
    for root, dirs, files in os.walk(jobs_folder):

        # Iterate over files
        for file in files:
            full_file_path = os.path.join(root, file)
            file_name, file_ext = os.path.splitext(file)
            job_id = os.path.basename(os.path.dirname(full_file_path))

            if file_ext == ".html":

                # Extract values such as job start date and time
                start = extract_job_start_date_time_line(file_path=full_file_path)
                dt_start_UTC = convert_start_date_time_to_datetime(value=start)
                # from_zone = dateutil.tz.tzutc()
                to_zone = dateutil.tz.tzlocal()
                dt_start_UTC.replace(tzinfo=to_zone)  # Not sure how this will be affected by time changes on my puter

                # Need master list of all dataframes, each containing the extracted html file values
                html_df = setup_initial_dataframe(file_path=full_file_path)
                html_df["JOB_ID"] = job_id  # Add a unique job id field to be able to group message content
                html_df.set_index(keys="JOB_ID", drop=True, inplace=True)
                master_html_df_list.append(html_df)

            elif file_ext == ".zip":

                # What is the compressed job size of the .zip file, if present
                byte_size = os.path.getsize(full_file_path) / 1000
                data = {"Name": [job_id], "ZIP Size KB": [byte_size]}
                master_zip_df_list.append(pd.DataFrame(data=data, dtype=str))

    # JOB HTML VALUES AS DATAFRAME
    try:
        master_html_values_df = pd.DataFrame(pd.concat(objs=master_html_df_list))
        # print(master_html_values_df.info())
    except ValueError:
        print("No .html files found.")

    # JOB ZIP FILE SIZE AS DATAFRAME
    try:
        master_zip_stats_df = pd.DataFrame(pd.concat(objs=master_zip_df_list))
        master_zip_stats_df.reset_index(drop=True, inplace=True)
        master_zip_stats_df["ZIP Size KB"] = pd.to_numeric(master_zip_stats_df["ZIP Size KB"])
        # print(master_zip_stats_df.info())
    except ValueError:
        print("No .zip files found.")

    # LEVEL SUMMARY
    # TODO: Refactor to function
    level_summary_list = process_level_summary_by_job(df=master_html_values_df)
    master_level_df = pd.DataFrame(pd.concat(objs=level_summary_list))
    master_level_df.reset_index(drop=False, inplace=True)
    master_level_df.rename(columns={"index": "Level", "Level": "Count"}, inplace=True)
    level_groupby_df = master_level_df.groupby(by=["JOB_ID", "Level"]).mean()
    # print(level_groupby_df.head())

    # EMAIL PROCESSING
    #   isolate the Message values that contain '@'
    emails_df = (extract_email_series_from_messages(df=master_html_values_df)
                 .to_frame(name="Email")
                 .reset_index())

    #   process email occurrences
    email_counts_df = count_email_occurrences(df=emails_df)
    # print(email_counts_df)

    #   process emails for unique extensions (gov, com, edu, etc)
    unique_email_extensions_df = determine_unique_email_extensions(df=email_counts_df)
    # print(unique_email_extensions_df)

    # ISSUING URL QUERY STRING EXTRACTION
    issuing_url_series = extract_issuing_url_series(df=master_html_values_df)
    issuing_url_query_string_dicts = extract_query_string_dicts(issuing_url_series)

    # CATALOG PROCESSING
    #   amount of url activity for catalogs
    catalog_url_activity_inventory_df = (issuing_url_query_string_dicts.apply(func=lambda x: x["cat"])
                                         .apply(func=lambda x: x[0])
                                         .value_counts()
                                         .to_frame()
                                         .reset_index())
    catalog_url_activity_inventory_df.rename(columns={"index": "Catalog URL Activity", "Message": "URL Calls"},
                                             inplace=True)
    # print(catalog_url_activity_inventory_df)

    #   count of job requests for catalogs
    catalog_job_request_count_df = inventory_catalog_job_request_count(df=master_html_values_df)
    # print(catalog_job_request_count_df)

    # Output various final contents to a unique sheet in excel file
    with pd.ExcelWriter(create_output_file_path(extension="xlsx")) as xlsx_writer:
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
        catalog_job_request_count_df.to_excel(excel_writer=xlsx_writer,
                                              sheet_name="Catalog Job Request Counts",
                                              na_rep=np.NaN,
                                              header=True,
                                              index=False)
        catalog_url_activity_inventory_df.to_excel(excel_writer=xlsx_writer,
                                                   sheet_name="Catalog URL Activty",
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


if __name__ == "__main__":
    main()
