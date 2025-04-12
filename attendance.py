from database_connect import DatabaseHandler
import tkinter as tk
from tkinter import filedialog
import pandas as pd
from collections import defaultdict
import os


class ReadEmployeeLog:
    def __init__(self):
        self.db = DatabaseHandler()  # Create DB instance
        file_path = self.get_file_path()
        if file_path:
            self.process_file(file_path)
            self.db.insert_attendance_records()


def get_file_path(self):
    # Get the directory where the script is running
    project_dir = os.path.dirname(
        os.path.abspath(__file__)
    )  # ✅ Get current project folder

    # Set the correct path to the default file inside attBot
    default_file = os.path.join(project_dir, "BQC2243300093_attlog.dat")  # ✅ Corrected

    # Check if the file exists
    if os.path.exists(default_file):
        return default_file

    def process_file(self, file_path):
        user_logs = []  # Store all records (no grouping)

        with open(file_path, "r") as file:
            rows = file.read().splitlines()

        for row in rows:
            try:
                nid, timestamp, event, device, access_code, flag = row.split("\t")
                user_logs.append([nid.strip(), timestamp.strip()])
            except ValueError:
                print(f"Skipping malformed row: {row}")

        # Convert to DataFrame
        df = pd.DataFrame(user_logs, columns=["UserID", "ClockInTime"])

        self.db.insert_into_db(df)  # Insert all records into the database


if __name__ == "__main__":
    ReadEmployeeLog()
