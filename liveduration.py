import requests
import dateutil.parser
from datetime import datetime, timezone
import mysql.connector
from flask import Flask, jsonify
import threading
import time
from mysql.connector import errorcode
from flask_cors import CORS
import pytz
import json
import re
app = Flask(__name__)
CORS(app)

with open('db_config.json', 'r') as f:
    db_config = json.load(f)

YOUTUBE_API_KEY = "AIzaSyA_rqcHrW1NyCJi6VPoApxSVHl7BZtse_8"
EVENT_ID = None
start_time = None
end_time = None

def get_youtube_live_times():
    global start_time, end_time
    event_id = fetch_event_id()
    if not event_id:
        return

    url = f"https://www.googleapis.com/youtube/v3/videos?part=liveStreamingDetails&id={EVENT_ID}&key={YOUTUBE_API_KEY}"

    try:
        response = requests.get(url)
        data = response.json()

        if 'items' not in data or len(data['items']) == 0:
            end_time = None
            print("Video not found.")
            return

        video_item = data['items'][0]

        if 'liveStreamingDetails' not in video_item:
            end_time = None
            print("Video is not a live video.")
            return
        
        live_streaming_details = video_item['liveStreamingDetails']

        # Check if the video is currently live
        if 'actualStartTime' in live_streaming_details:
            if not start_time:
                start_time = dateutil.parser.isoparse(live_streaming_details['actualStartTime'])
                # Insert the start time in the database
                insert_start_time(start_time, EVENT_ID)

            # Check if the video is completed
            if 'actualEndTime' in live_streaming_details:
                end_time = dateutil.parser.isoparse(live_streaming_details['actualEndTime'])
                # Update the end time in the database
                update_end_time(end_time, EVENT_ID)
            else:
                # The video is still ongoing and has not ended
                end_time = None
        else:
            print("The video is not currently live.")

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        end_time = None

def insert_start_time(start_time, event_id):
    ist_tz = pytz.timezone('Asia/Kolkata')
    start_time_ist = start_time.astimezone(ist_tz)

    connection = mysql.connector.connect(
        host=db_config["database"]["host"],
        database=db_config["database"]["database_name"],
        user=db_config["database"]["user"],
        password=db_config["database"]["password"],
        auth_plugin=db_config["database"]["auth_plugin"]
    )
    cursor = connection.cursor()

    insert_query = "INSERT INTO live_stream_data (video_id, start_time) VALUES (%s, %s);"
    try:
        cursor.execute(insert_query, (event_id, start_time_ist))
        connection.commit()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            update_query = "UPDATE live_stream_data SET start_time = %s WHERE video_id = %s;"
            cursor.execute(update_query, (start_time_ist, event_id))
            connection.commit()

        else:
            print(f"An error occurred: {err}")


    cursor.close()
    connection.close()

def update_end_time(end_time, event_id):
    global start_time
    youtube_link=fetch_event_id()
    ist_tz = pytz.timezone('Asia/Kolkata')
    end_time_ist = end_time.astimezone(ist_tz)
    connection = mysql.connector.connect(
       host=db_config["database"]["host"],
       database=db_config["database"]["database_name"],
       user=db_config["database"]["user"],
       password=db_config["database"]["password"],
       auth_plugin=db_config["database"]["auth_plugin"]
    )
    cursor = connection.cursor()

    update_query = "UPDATE live_stream_data SET end_time = %s WHERE video_id = %s;"
    cursor.execute(update_query, (end_time_ist, event_id))
    start_time_ist = start_time.astimezone(ist_tz)

    # Calculate and update the duration
    duration = (end_time_ist - start_time_ist).total_seconds() if start_time_ist else None
    update_query = "UPDATE live_stream_data SET duration = %s WHERE video_id = %s;"
    cursor.execute(update_query, (duration, event_id))
    connection.commit()
    update_query = "UPDATE student_watched_hours_deatils SET video_duration = %s WHERE video_id = %s;"
    cursor.execute(update_query, (duration, event_id))
    connection.commit()

    cursor.close()
    connection.close()
    return duration,end_time


def fetch_event_id():
    connection = mysql.connector.connect(
       host=db_config["database"]["host"],
       database=db_config["database"]["database_name"],
       user=db_config["database"]["user"],
       password=db_config["database"]["password"],
       auth_plugin=db_config["database"]["auth_plugin"]
    )
    cursor = connection.cursor()

    select_query = "SELECT youtube_link  FROM youtube_link_table LIMIT 1;"
    cursor.execute(select_query)
    result = cursor.fetchone()
    youtube_link = result[0] if result else None
    if youtube_link is not None:
        pattern = re.compile(r'(?:live/|embed/|v=|youtu.be/)([^"&?\/\s]{11})')
        match = pattern.search(youtube_link)
        event_id = match.group(1) if match else None
    else:
        event_id = None

    print(event_id)
    cursor.close()
    connection.close()
    
    return event_id, youtube_link

def monitor_live_stream():
    global EVENT_ID,  start_time, end_time
    last_youtube_link = None
    while True:
        # Check for changes in the YouTube link
        current_event_id,current_youtube_link = fetch_event_id()
        
        if current_event_id != EVENT_ID:
            # Reset monitoring with the new link and monitoring interval
            EVENT_ID = current_event_id if current_event_id else None
            last_youtube_link = current_youtube_link
            start_time = None
            end_time = None

            print(f"Monitoring new YouTube link: {current_event_id}")

        if EVENT_ID:
            get_youtube_live_times()

        if end_time:
            print("Live stream ended. Stopping monitoring.")
            duration=update_end_time(end_time, EVENT_ID)
            print("duration",duration[0])
            api_url = "http://uat.traditionalyoga.co.in/yogaStudent/student/endOfLive"
            payload = {
                "liveId": EVENT_ID,  # Replace with the correct value
                "videoDuration": duration[0]  # Replace with the correct value
            }
            headers = {"Content-Type": "application/json"}
            response = requests.post(api_url, json=payload, headers=headers)
            
            if response.status_code == 200:
                print("API call successful.")
            else:
                print("API call failed with status code:", response.status_code)

            remove_live_link()
            
            break

        else:

           time.sleep(900)  # 60 seconds in sleep (default if monitoring_interval is not found)

    print("Live stream ended. Stopping monitoring.")

def remove_live_link():
    
    connection = mysql.connector.connect(
        host=db_config["database"]["host"],
        database=db_config["database"]["database_name"],
        user=db_config["database"]["user"],
        password=db_config["database"]["password"],
        auth_plugin=db_config["database"]["auth_plugin"]
    )
    cursor = connection.cursor()
    
    update_query = "update youtube_link_table SET youtube_link =%s where link_id=1;"
    cursor.execute(update_query,(None,))
    
    connection.commit()
    cursor.close()
    connection.close()

@app.route('/get_live_times')
def get_live_times():
    global start_time, end_time

    if not EVENT_ID:
        return jsonify({"error": "No event ID found. Make sure to call fetch_event_id() first."})

    # Define the timezone for IST (Indian Standard Time)
    ist_tz = pytz.timezone('Asia/Kolkata')

    if start_time and not end_time:
        start_time_ist = start_time.astimezone(ist_tz)
        response = {
            "event_id": EVENT_ID,
            "start_time": start_time_ist.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": None,
            "duration": None
        }
        return jsonify(response)

    if end_time:
        start_time_ist = start_time.astimezone(ist_tz)
        end_time_ist = end_time.astimezone(ist_tz)
        duration = (end_time_ist - start_time_ist).total_seconds() if start_time else None
        response = {
            "event_id": EVENT_ID,
            "start_time": start_time_ist.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": end_time_ist.strftime('%Y-%m-%d %H:%M:%S'),
            "duration": duration
        }

        return jsonify(response)
    else:
        return jsonify({"error": "The video is not live or not found."})


if __name__ == '__main__':
    last_youtube_link = None  # Initialize last_youtube_link
    # Start the background thread to monitor the live stream
    monitor_thread = threading.Thread(target=monitor_live_stream)
    monitor_thread.daemon = True
    monitor_thread.start()

    # Start the Flask app
    app.run(host='0.0.0.0', port=5001)
