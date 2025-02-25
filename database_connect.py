import mysql.connector


class DatabaseHandler:
    def __init__(
        self, host="localhost", database="ja_db", user="root", password="2044"
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.dbConn = self.connect_to_database()

    def connect_to_database(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print("✅ Database connection successful")
            return conn
        except mysql.connector.Error as err:
            print(f"❌ Failed to connect to MySQL: {err}")
            return None  # Return None if connection fails

    def insert_into_db(self, df):
        if not self.dbConn:  # Check if connection exists
            print("❌ Database connection not established!")
            return

        try:
            cursor = self.dbConn.cursor()

            # Query to fetch emp_id using document_no
            emp_lookup_query = "SELECT emp_id FROM hrm_employees WHERE document_no = %s"

            # Insert query (each timestamp as a separate record)
            insert_query = """
                INSERT INTO hrm_payroll_clockings (emp_id, clockedin_time)
                VALUES (%s, %s);
                """

            for _, row in df.iterrows():
                document_no = row["UserID"]  # Assuming "UserID" holds document_no

                # Fetch emp_id using document_no
                cursor.execute(emp_lookup_query, (document_no,))
                result = cursor.fetchone()

                if result:  # If a matching emp_id is found
                    emp_id = result[0]  # Extract emp_id from result tuple

                    # Insert each timestamp as a separate record
                    cursor.execute(insert_query, (emp_id, row["ClockInTime"]))

                else:
                    print(f"⚠️ No matching emp_id found for document_no: {document_no}")

            self.dbConn.commit()
            cursor.close()
            print("✅ All clocking records inserted into MySQL!")

        except Exception as e:
            print(f"❌ Error inserting data: {e}")
