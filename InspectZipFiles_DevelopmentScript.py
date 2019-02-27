import os

# job_dir = r"export_dir"
job_dir = r"D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir"

for root, dirnames, files in os.walk(job_dir):

    for file in files:

        full_path = os.path.join(root, file)
        file_name, file_ext = os.path.splitext(full_path)
        if file_ext == ".zip":
            print(file)
            print("\t", os.path.getsize(full_path)/1000, " KB")

