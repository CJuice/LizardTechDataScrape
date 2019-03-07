"""
Walk the specified directory, checking the age of folders and files, deleting all that are greater than 30 days in age.
Walk the LizardTech Output folder on Lidar Server. Check the modified date of all folders and files. Delete all items
that are older than 30 days in age. Print out indications of age and item path and name that are deleted so that there
is transparency.
Date Created:
Author: CJuice
Revisions:
NOTE: Forked from AGS_File_Bloat_Reduction

"""


def main():
    import datetime
    import os
    import shutil

    root_project_path = os.path.dirname(__file__)   # DEVELOPMENT
    DIRECTORY_TO_EXAMINE = os.path.join(root_project_path, "export_dir2")    # DEVELOPMENT

    # DIRECTORY_TO_EXAMINE = r'D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir'  # PRODUCTION
    AGE_COMPARISON_VALUE = datetime.timedelta(days=30)
    now = datetime.datetime.now()

    try:
        for root, dirnames, files in os.walk(DIRECTORY_TO_EXAMINE):

            # For each directory, look at the residing folders and files. Begin with folders first.
            for folder in dirnames:
                full_folder_path = os.path.join(root, folder)
                time_dir_last_modified = os.path.getmtime(full_folder_path)
                duration_since_folder_last_modified = now - datetime.datetime.fromtimestamp(time_dir_last_modified)
                print("Folder: {} , Age: {}".format(full_folder_path, duration_since_folder_last_modified))
                is_older_than_age_comparison_val = duration_since_folder_last_modified > AGE_COMPARISON_VALUE
                if is_older_than_age_comparison_val:
                    try:
                        # Note: os.remove() supposedly doesn't work on folders. os.removedirs() doesn't work on
                        #   non-empty folders. Use shutil.rmtree() to remove folders with content.
                        shutil.rmtree(full_folder_path)
                        print("REMOVED {} - Age: {}\n".format(full_folder_path, duration_since_folder_last_modified))
                    except Exception as e:
                        print("\tALERT: {} NOT REMOVED. EXCEPTION! {}\n".format(full_folder_path, e))

            for file in files:

                # For files in the directory, process them.
                full_file_path = os.path.join(root, file)
                time_file_last_modified = os.path.getmtime(full_file_path)
                duration_since_file_last_modified = now - datetime.datetime.fromtimestamp(time_file_last_modified)
                print("File: {} , Age: {}".format(full_folder_path, duration_since_file_last_modified))
                is_older_than_age_comparison_val = duration_since_file_last_modified > AGE_COMPARISON_VALUE
                if is_older_than_age_comparison_val:
                    try:
                        os.remove(full_file_path)
                        print("REMOVED {} - Age: {}\n".format(full_file_path, duration_since_file_last_modified))
                    except Exception as e:
                        print("\tALERT: {} NOT REMOVED. EXCEPTION! {}\n".format(full_file_path, e))

    except IOError as io_err:
        print(io_err)
        exit()
    except Exception as e:
        print(e)
        exit()


if __name__ == "__main__":
    main()

