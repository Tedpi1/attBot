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
                INSERT INTO hrm_payroll_clockingss (emp_id, clockedin_time)
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

    def insert_attendance_records(self):
        if not self.dbConn:
            print("❌ Database connection not established!")
            return

        try:
            cursor = self.dbConn.cursor()

            # SQL Query to fetch clock-in and clock-out records
            fetch_query = """
                SELECT 
                    p.emp_id,  
                    MIN(p.clockedin_time) AS off_clocked_in,  
                    CASE  
                        WHEN COUNT(p.clockedin_time) = 1 THEN NULL  
                        ELSE MAX(p.clockedin_time)  
                    END AS off_clocked_out  
                FROM hrm_payroll_clockingss p  
                WHERE p.clockedin_time IS NOT NULL  
                GROUP BY p.emp_id, DATE(p.clockedin_time);
            """

            # Execute fetch query
            cursor.execute(fetch_query)
            records = cursor.fetchall()

            if not records:
                print("⚠️ No records found to insert.")
                return

            # Insert query
            insert_query = """
                INSERT INTO hrm_payroll_attendance_clocking_time (emp_id, off_clocked_in, off_clocked_out)
                VALUES (%s, %s, %s);
            """

            # Insert each record
            cursor.executemany(insert_query, records)

            # Commit transaction
            self.dbConn.commit()
            print(f"✅ {cursor.rowcount} records inserted successfully!")

        except mysql.connector.Error as err:
            print(f"❌ Error inserting data: {err}")

        finally:
            cursor.close()
