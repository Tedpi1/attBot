from database_connect import DatabaseHandler
import tkinter as tk
from tkinter import filedialog
import pandas as pd
from collections import defaultdict


class ReadEmployeeLog:
    def __init__(self):
        self.db = DatabaseHandler()  # Create DB instance
        file_path = self.get_file_path()
        if file_path:
            self.process_file(file_path)

    def get_file_path(self):
        root = tk.Tk()
        root.withdraw()
        return filedialog.askopenfilename(
            title="Select a log file",
            filetypes=[("DAT files", "*.dat"), ("All files", "*.*")],
        )

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
