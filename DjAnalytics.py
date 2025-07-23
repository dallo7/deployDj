import psycopg2
import psycopg2.extras
import json
import os
import sys
from datetime import datetime, timedelta, timezone

# --- Database Connection Details ---

DB_HOST = "beatbnk-db.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com"
DB_NAME = "beatbnk_db"
DB_USER = "user"
DB_PASS = "X1SOrzeSrk"
DB_PORT = "5432"


# --- Helper Functions for Each Report Component ---


def get_performer_id_by_name(dj_name: str) -> int | None:
    """
    Finds a DJ's unique performer ID by their name.

    This function connects to the database, joins the users and performers tables,
    and retrieves the performer ID based on the user's name and role.

    Args:
        dj_name: The case-sensitive name of the DJ to find.

    Returns:
        The integer performer ID if found, otherwise None.
    """
    conn = None
    performer_id = None
    print(f"\n🔎 Searching for performer ID for DJ: '{dj_name}'...")

    sql = """
        SELECT
            p.id
        FROM
            users u
        JOIN
            performers p ON u.id = p."userId"
        WHERE
            u.name = %(dj_name)s AND u.role = 'DJ'
        LIMIT 1;
    """

    try:
        # Establish a connection to the database
        print(f"  - Connecting to database '{DB_NAME}'...")
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Execute the query
        print(f"  - Executing query to find user '{dj_name}' with role 'DJ'...")
        cur.execute(sql, {'dj_name': dj_name})
        result = cur.fetchone()

        # Process the result
        if result:
            performer_id = result['id']
            print(f"  ✅ Found Performer ID: {performer_id}")
        else:
            print(f"  ⚠️  Warning: No performer found with the name '{dj_name}'.")

        cur.close()

    except psycopg2.OperationalError as e:
        print(f"  ❌ DATABASE CONNECTION ERROR: Could not connect.")
        print(f"     Error details: {e}")
        return None
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"  ❌ An unexpected database error occurred: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()
            print("  - Database connection closed.")

    return performer_id


def get_header_stats(cur, performer_id: int, dj_user_id: int) -> dict:
    """Gets the main stats: DJ name, followers, total requests, and played requests."""
    sql = """
        SELECT
            (SELECT name FROM users WHERE id = %(user_id)s) AS dj_name,
            (SELECT "profileImageUrl" FROM users WHERE id = %(user_id)s) as dj_image_url,
            (SELECT COUNT(*) FROM follows WHERE "entityId" = %(user_id)s AND "entityType" = 'DJ' AND "deletedAt" IS NULL) AS total_followers,
            (SELECT COUNT(*) FROM song_requests WHERE "performerId" = %(performer_id)s) AS total_requests,
            (SELECT COUNT(*) FROM song_requests WHERE "performerId" = %(performer_id)s AND UPPER(status::text) = 'PLAYED') AS played_requests;
    """
    cur.execute(sql, {'user_id': dj_user_id, 'performer_id': performer_id})
    result = cur.fetchone()
    return dict(result) if result else {}


def get_time_filtered_stats(cur, performer_id: int, dj_user_id: int, start_date: datetime, end_date: datetime) -> dict:
    """Calculates stats for a specific time period (e.g., last 30 days)."""
    sql = """
        SELECT
            (SELECT COUNT(*) FROM follows WHERE "entityId" = %(user_id)s AND "createdAt" BETWEEN %(start_date)s AND %(end_date)s) AS new_followers,
            (SELECT COUNT(*) FROM follows WHERE "entityId" = %(user_id)s AND "deletedAt" BETWEEN %(start_date)s AND %(end_date)s) AS unfollows,
            COUNT(id) FILTER (WHERE UPPER(status::text) = 'ACCEPTED') AS accepted_requests,
            COUNT(id) FILTER (WHERE UPPER(status::text) = 'REJECTED') AS rejected_requests
        FROM song_requests
        WHERE "performerId" = %(performer_id)s AND "createdAt" BETWEEN %(start_date)s AND %(end_date)s;
    """
    params = {'user_id': dj_user_id, 'performer_id': performer_id, 'start_date': start_date, 'end_date': end_date}
    cur.execute(sql, params)
    result = cur.fetchone()
    return dict(result) if result else {}


def get_requests_per_month(cur, performer_id: int, months: int = 6) -> list:
    """Gets the count of new song requests per month for the last N months."""
    start_date = datetime.now(timezone.utc) - timedelta(days=months * 30)
    sql = """
        SELECT TO_CHAR(DATE_TRUNC('month', "createdAt"), 'YYYY-MM') AS month, COUNT(id) AS request_count
        FROM song_requests WHERE "performerId" = %(performer_id)s AND "createdAt" >= %(start_date)s
        GROUP BY month ORDER BY month;
    """
    cur.execute(sql, {'performer_id': performer_id, 'start_date': start_date})
    return [dict(row) for row in cur.fetchall()]


def get_most_accepted_genres(cur, performer_id: int, limit: int = 5) -> list:
    """Finds the most accepted genres for a DJ, including percentages."""
    sql = """
        WITH total_accepted AS (
            SELECT COUNT(*) AS total FROM song_requests WHERE "performerId" = %(performer_id)s AND UPPER(status::text) = 'ACCEPTED'
        )
        SELECT g.name AS genre_name, COUNT(sr.id) AS accepted_count, ROUND((COUNT(sr.id) * 100.0) / NULLIF((SELECT total FROM total_accepted), 0), 2) AS percentage
        FROM song_requests sr JOIN genres g ON sr."genreId" = g.id
        WHERE sr."performerId" = %(performer_id)s AND UPPER(sr.status::text) = 'ACCEPTED'
        GROUP BY g.name ORDER BY accepted_count DESC LIMIT %(limit)s;
    """
    cur.execute(sql, {'performer_id': performer_id, 'limit': limit})
    return [dict(row) for row in cur.fetchall()]


def get_top_supporters(cur, performer_id: int, limit: int = 5) -> list:
    """Finds top supporters by total tip amount."""
    sql = """
        SELECT u.name AS supporter_name, u."profileImageUrl", SUM(pt."tipAmount") AS total_tipped
        FROM performer_tips AS pt JOIN users AS u ON pt."userId" = u.id
        WHERE pt."performerId" = %(performer_id)s
        GROUP BY u.id, u.name, u."profileImageUrl" ORDER BY total_tipped DESC LIMIT %(limit)s;
    """
    cur.execute(sql, {'performer_id': performer_id, 'limit': limit})
    return [dict(row) for row in cur.fetchall()]


def get_earnings_per_month(cur, performer_id: int, months: int = 6) -> list:
    """Calculates total earnings (from tips and tipped requests) per month."""
    start_date = datetime.now(timezone.utc) - timedelta(days=months * 30)
    sql = """
        WITH all_earnings AS (
            SELECT "createdAt", "tipAmount" FROM performer_tips WHERE "performerId" = %(performer_id)s
            UNION ALL
            SELECT "createdAt", "tipAmount" FROM song_requests WHERE "performerId" = %(performer_id)s AND "tipAmount" > 0
        )
        SELECT TO_CHAR(DATE_TRUNC('month', "createdAt"), 'YYYY-MM') AS month, COALESCE(SUM("tipAmount"), 0) AS total_earnings
        FROM all_earnings WHERE "createdAt" >= %(start_date)s GROUP BY month ORDER BY month;
    """
    cur.execute(sql, {'performer_id': performer_id, 'start_date': start_date})
    return [dict(row) for row in cur.fetchall()]


# --- Main Orchestration Function ---

def generate_dj_analytics_report(dj_performer_id: int) -> dict:
    """
    Connects to the database and calls all helper functions to build a complete
    analytics report for a single DJ.
    """
    conn = None
    report = {}
    print("🚀 Starting analytics report generation...")

    try:
        # Step 1: Connect to the database
        print(f"  Attempting to connect to database '{DB_NAME}' at '{DB_HOST}'...")
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("  ✅ Database connection successful.\n")

        # Step 2: Get the performer's associated user ID
        print(f"  🔎 Fetching user ID for performer ID: {dj_performer_id}...")
        cur.execute('SELECT "userId" FROM performers WHERE id = %(id)s', {'id': dj_performer_id})
        result = cur.fetchone()
        if not result:
            print(f"  ❌ Error: Performer with ID {dj_performer_id} not found.")
            return {"error": f"Performer with ID {dj_performer_id} not found."}
        dj_user_id = result['userId']
        print(f"  ✅ Found User ID: {dj_user_id}\n")

        # Step 3: Fetch all report components
        print("--- Fetching Report Components ---")

        print("  📊 Fetching header stats...")
        header_stats = get_header_stats(cur, dj_performer_id, dj_user_id)
        print("  ...done.")

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        print(f"  📈 Fetching time-filtered stats (for last 30 days)...")
        time_filtered_stats = get_time_filtered_stats(cur, dj_performer_id, dj_user_id, start_date, end_date)
        print("  ...done.")

        print("  🎵 Fetching most accepted genres...")
        most_accepted_genres = get_most_accepted_genres(cur, dj_performer_id)
        print("  ...done.")

        print("  ❤️  Fetching top supporters...")
        top_supporters = get_top_supporters(cur, dj_performer_id)
        print("  ...done.")

        print("  📅 Fetching monthly requests...")
        requests_over_time = get_requests_per_month(cur, dj_performer_id)
        print("  ...done.")

        print("  💰 Fetching monthly earnings...")
        earnings_over_time = get_earnings_per_month(cur, dj_performer_id)
        print("  ...done.\n")

        # Step 4: Assemble the final report
        print("--- Assembling Final Report ---")
        report = {
            "generated_at": end_date.isoformat(), "performer_id": dj_performer_id,
            "dj_info": {"name": header_stats.get('dj_name'), "profile_image_url": header_stats.get('dj_image_url')},
            "header_stats": {"total_followers": header_stats.get('total_followers'),
                             "total_requests": header_stats.get('total_requests'),
                             "played_requests": header_stats.get('played_requests')},
            "analytics_page": {"period": f"{start_date.date()} to {end_date.date()}", "stats": time_filtered_stats},
            "charts": {"requests_over_time": requests_over_time, "earnings_over_time": earnings_over_time,
                       "most_accepted_genres": most_accepted_genres},
            "top_supporters": top_supporters
        }
        print("  ✅ Final report dictionary assembled successfully.\n")
        cur.close()

    except psycopg2.OperationalError as e:
        print(f"❌ DATABASE CONNECTION ERROR: Could not connect to the database.")
        print(
            f"   Please check if the database is running and if the credentials in your environment variables are correct.")
        print(f"   Error details: {e}")
        return {"error": "Database connection failed."}
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ An unexpected error occurred: {error}")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()
            print("🚪 Database connection closed.")

    return report


# --- Example Usage ---

if __name__ == '__main__':
    # Check if all required environment variables are set before running
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS", "DB_PORT"]
    if not all(var for var in required_vars):
        print("❌ Error: Not all required database environment variables are set.")
        print("   Please set the following variables:", ", ".join(required_vars))
        sys.exit(1)  # Exit the script if config is missing
    else:
        target_dj_id = get_performer_id_by_name("DJ Marlone")
        print("=====================================================")
        print(f"   Starting Full Analytics Run for Performer ID: {target_dj_id}")
        print("=====================================================")

        analytics_data = generate_dj_analytics_report(target_dj_id)

        print("\n=====================================================")
        if "error" in analytics_data:
            print("   ❌ Report generation failed.")
        else:
            print("   ✅ Report generation complete. Final JSON output:")
            print("=====================================================")
            # Convert the dictionary to a JSON string for API response or file storage
            json_output = json.dumps(analytics_data, indent=4, default=str)
            print(json_output)
