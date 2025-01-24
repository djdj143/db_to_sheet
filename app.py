from flask import Flask, request, jsonify
import pymysql
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# Google Sheets API setup
def get_google_sheets_service():
    credentials = Credentials.from_service_account_file("rsa.json", scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=credentials)

def write_to_google_sheet(sheet_id, data, sheet_range):
    try:
        service = get_google_sheets_service()
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

# Database connection function
def get_db_connection(host, user, password, database):
    try:
        return pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
    except Exception as e:
        return str(e)

@app.route("/api", methods=["POST"])
def process_data():
    try:
        # Extract data from the request
        request_data = request.json
        sheet_id = request_data.get("sheetid")
        sheet_range = request_data.get("range")
        query = request_data.get("qry")

        # Database connection details
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
        data = [list(row) for row in results]

        # Write to Google Sheets
        result = write_to_google_sheet(sheet_id, data, sheet_range)

        return jsonify({"status": "success", "message": result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/test", methods=["GET"])
def test_api():
    return jsonify({"status": "success", "message": "API is running!"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
