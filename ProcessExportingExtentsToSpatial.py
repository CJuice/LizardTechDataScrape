"""

"""
import pandas as pd
import numpy as np

file_to_examine = r"GrabLizardTechOutputLogInfo\ForSpatialTest.xlsx"
sheet_to_examine = "QP - Exporting Extent"

def process_raw_extent_value(val):
    val_list = val.split(",")
    val_list.pop(5)
    val_list.pop(2)
    return val_list


exporting_extent_df = pd.read_excel(io=file_to_examine, sheet_name=sheet_to_examine, header=0)

exporting_extent_df["Exporting Extent"] = exporting_extent_df["Exporting Extent"].apply(process_raw_extent_value)

print(exporting_extent_df)

# For real implementation, need EPSG so the coordinates can be interpreted