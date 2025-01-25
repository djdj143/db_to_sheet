from flask import Flask, request, jsonify
import pymysql
import json
import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# Load Google Service Account Credentials
def load_google_credentials():
    try:
        with open("rsa.json", "r") as file:
            credentials_data = json.load(file)

        # Ensure private_key is properly formatted
        if "private_key" not in credentials_data or not credentials_data["private_key"]:
            raise ValueError("❌ Private key is missing or incorrectly formatted!")

        # Fix formatting if necessary
        if not credentials_data["private_key"].startswith("-----BEGIN PRIVATE KEY-----"):
            credentials_data["private_key"] = "-----BEGIN PRIVATE KEY-----\n" + credentials_data["private_key"] + "\n-----END PRIVATE KEY-----"

        # Authenticate with Google API
        credentials = Credentials.from_service_account_info(
            credentials_data,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )

        return build("sheets", "v4", credentials=credentials)

    except Exception as e:
        print(f"❌ Error loading credentials: {e}")
        return None

# Write Data to Google Sheets
def write_to_google_sheet(sheet_id, data, sheet_range):
    try:
        service = load_google_credentials()
        if not service:
            return "❌ Google Sheets authentication failed!"

        body = {"values": data}

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=sheet_range,
            valueInputOption="RAW",
            body=body
        ).execute()

        return "✅ Data written successfully!"
    except Exception as e:
        return str(e)

# Connect to MySQL Database
def get_db_connection(host, user, password, database):
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            connect_timeout=60,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.MySQLError as e:
        return f"❌ MySQL Connection Error: {str(e)}"

# API Endpoint: Process Data
@app.route("/api", methods=["POST"])
def process_data():
    try:
        request_data = request.json
        sheet_id = request_data.get("sheetid")
        sheet_range = request_data.get("range")
        query = request_data.get("qry")

        db_host = request_data.get("host")
        db_user = request_data.get("user")
        db_password = request_data.get("password")
        db_name = request_data.get("database")

        # Validate required fields
        if not all([sheet_id, sheet_range, query, db_host, db_user, db_password, db_name]):
            return jsonify({"status": "error", "message": "❌ Missing required fields!"}), 400

        # Connect to the database
        connection = get_db_connection(db_host, db_user, db_password, db_name)
        if isinstance(connection, str):  # If connection returns an error string
            return jsonify({"status": "error", "message": connection}), 500

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()

        # Convert date fields to string format
        for row in results:
            for key, value in row.items():
                if isinstance(value, (datetime.date, datetime.datetime)):
                    row[key] = value.strftime("%Y-%m-%d")  # Convert date to "YYYY-MM-DD"

        # Convert results to a list of lists for Google Sheets API
        data = [list(row.values()) for row in results]

        # Write to Google Sheets
        result = write_to_google_sheet(sheet_id, data, sheet_range)

        return jsonify({"status": "success", "message": result}), 200

    except pymysql.MySQLError as db_err:
        return jsonify({"status": "error", "message": f"❌ Database Error: {str(db_err)}"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"❌ Internal Server Error: {str(e)}"}), 500

# Run the Flask App
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
