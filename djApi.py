from flask import Flask, jsonify
from flask_cors import CORS 
from DjAnalytics import get_performer_id_by_name, generate_dj_analytics_report 

app = Flask(__name__)
CORS(app) 

@app.route("/api/dj/analytics/<string:dj_name>", methods=["GET"])
def get_dj_report(dj_name: str):
    """
    API endpoint to get an analytics report for a specific DJ by name.
    """
    print(f"Received request for DJ: {dj_name}")

    performer_id = get_performer_id_by_name(dj_name)

    if performer_id is None:
        error_message = {"error": f"DJ with name '{dj_name}' not found."}
        return jsonify(error_message), 404

    report_data = generate_dj_analytics_report(performer_id)

    if "error" in report_data:
        return jsonify(report_data), 500

    print(f"Successfully generated report for DJ: {dj_name}")
    return jsonify(report_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5070, debug=False, use_reloader=False)
