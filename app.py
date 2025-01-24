from flask import Flask, request, jsonify
import pymysql
import json
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# Load Google Sheets API credentials from rsa.json
def get_google_sheets_service():
    try:
        with open("rsa.json", "r") as file:
            credentials_data = json.load(file)

        credentials = Credentials.from_service_account_info(
            credentials_data,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return build("sheets", "v4", credentials=credentials)
    except Exception as e:
        return str(e)

# Function to write data to Google Sheets
def write_to_google_sheet(sheet_id, data, sheet_range):
    try:
        service = get_google_sheets_service()
        if isinstance(service, str):  # If service returns an error string
            return service

        body = {"values": data}

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=sheet_range,
            valueInputOption="RAW",
            body=body
        ).execute()

        return "Data written successfully"
    except Exception as e:
        return str(e)

# Function to connect to MySQL database
def get_db_connection(host, user, password, database):
    try:
        return pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        return str(e)

# Main API route to process data from MySQL to Google Sheets
@app.route("/api", methods=["POST"])
def process_data():
    try:
        # Extract data from request
        request_data = request.json
        sheet_id = request_data.get("sheetid")
        sheet_range = request_data.get("range")
        query = request_data.get("qry")

        # Database connection details from request
        db_host = request_data.get("host")
        db_user = request_data.get("user")
        db_password = request_data.get("password")
        db_name = request_data.get("database")

        # Validate required fields
        if not all([sheet_id, sheet_range, query, db_host, db_user, db_password, db_name]):
            return jsonify({"status": "error", "message": "Missing required fields"}), 400

        # Connect to the database
        connection = get_db_connection(db_host, db_user, db_password, db_name)
        if isinstance(connection, str):  # If connection returns an error string
            return jsonify({"status": "error", "message": connection}), 500

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        connection.close()

        # Convert results to a list of lists for Google Sheets API
        data = [list(row.values()) for row in results]

        # Write to Google Sheets
        result = write_to_google_sheet(sheet_id, data, sheet_range)

        return jsonify({"status": "success", "message": result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Health check route
@app.route("/test", methods=["GET"])
def test_api():
    return jsonify({"status": "success", "message": "API is running!"}), 200

# Run the Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
