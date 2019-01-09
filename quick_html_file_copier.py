import shutil
import os


job_folder = r"D:\Program Files\LizardTech\Express Server\ImageServer\var\export_dir"
destination = r"D:\Program Files\LizardTech\Express Server\ImageServer\var\tempcopies"
for dirname, dirs, files in os.walk(job_folder):
    for file in files:
        if file.endswith(".html"):
            src = os.path.join(dirname, file)
            shutil.copy(src=src, dst=destination)
            print(src)
