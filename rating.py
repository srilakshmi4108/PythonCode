# import necessary libraries
# The below line imports the pandas library, which is used for data analysis.
import pandas as pd 
# This below line 6 imports the flask library, which is used for creating web applications.
#This below line 6 imports the request library, which is used to get information from the user.
# This below line 6 imports the jsonify library, which is used to convert Python objects into JSON format.
from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
from sqlalchemy.ext.declarative import declarative_base
import random
import datetime
from textblob import TextBlob
import numpy as np
# disable pandas warnings
pd.options.mode.chained_assignment = None
mydb = mysql.connector.connect(
  host="15.207.1.98",
  database='uatdb-ty',
  user= "uatdbmngr",
  password="%Pvji1!v*7#0",
  auth_plugin='mysql_native_password'
  
)

print("Connected dev")

    # pass

# connect to MySQL database
db_cursor = mydb.cursor()


# retrieve all rows from the m_courses table and convert the dta and attach column names
db_cursor.execute('SELECT * FROM m_courses')

table_rows = db_cursor.fetchall()

m_courses = pd.DataFrame(table_rows)

db_cursor.execute("SHOW columns FROM m_courses")

cols = [column[0] for column in db_cursor.fetchall()]

m_courses.columns = cols

# Subset the m_courses and get only ongoing ciurses

# m_courses = m_courses[m_courses["status"]=="ongoing"]["courses_id"].values



# retrieve all rows from the student_registration table and convert the dta and attach column names
db_cursor.execute('SELECT * FROM student_registration')

table_rows = db_cursor.fetchall()

registration = pd.DataFrame(table_rows)

db_cursor.execute("SHOW columns FROM student_registration")

cols = [column[0] for column in db_cursor.fetchall()]

registration.columns = cols
# retrieve all rows from the gratitude_messages table and convert the dta and attach column names
db_cursor.execute('SELECT * FROM gratitude_messages')

table_rows = db_cursor.fetchall()

gratitude = pd.DataFrame(table_rows)

db_cursor.execute("SHOW columns FROM gratitude_messages")

cols = [column[0] for column in db_cursor.fetchall()]

gratitude.columns = cols
# retrieve all rows from the performance_rating table and convert the dta and attach column names
db_cursor.execute('SELECT * FROM performace_rating')

table_rows = db_cursor.fetchall()

performace = pd.DataFrame(table_rows)

db_cursor.execute("SHOW columns FROM performace_rating")

cols = [column[0] for column in db_cursor.fetchall()]

performace.columns = cols


query='SELECT DISTINCT * FROM performace_rating p INNER JOIN m_courses m ON p.course_id=m.courses_id where parameters_id IN (1,4,7) and m.status="ongoing"'
db_cursor.execute(query)
results = db_cursor.fetchall()
course_id1=[result[1] for result in results]
print(course_id1)
query='SELECT registration_id,s.course_id FROM student_registration s INNER JOIN m_courses m ON s.course_id=m.courses_id INNER JOIN student_mentor_chief_mentor_mapping SM ON SM.student_id=s.registration_id where student_status in (1,5) and m.status="ongoing"'
db_cursor.execute(query)
results1 = db_cursor.fetchall()
query='SELECT student_id,COUNT(*) AS message_count FROM gratitude_messages WHERE student_message_status=1 and gratitude_admin_circulated_message_status=20 and student_sent_date >= DATE_SUB(CURDATE(), INTERVAL 15 DAY) GROUP BY student_id'
db_cursor.execute(query)
results2 = db_cursor.fetchall()



# Execute the stored procedure
fourth_column_values = []
fourth_column_list = []
student_averages = []

student_id=[result[0] for result in results1]
course_id=[result[1] for result in results1]
for student, course in zip(student_id, course_id):
    try:
        mydb = mysql.connector.connect(
          host="15.207.1.98",
          database='uatdb-ty',
          user= "uatdbmngr",
          password="%Pvji1!v*7#0",
          auth_plugin='mysql_native_password'
          
        )
        db_cursor = mydb.cursor()
        query = "CALL student_performance(%s, %s)"
        db_cursor.execute(query, [student, course])
        result_set = db_cursor.fetchall()
        if result_set:
            values = [(student, row[3],course) for row in result_set[:2]]
            fourth_column_values.extend(values)
            values = [row[3] for row in result_set[:2]]

# Check for None values in the rows and replace with 0
            values = [value if value is not None else 0 for value in values]
            
            total_value = sum(values)
            average_value = total_value / 2 
            
            # Append the average value to the list
            student_averages.append((student, average_value))

           
        else:
            print(f"No results for student {student} and course {course}")


        
    except Exception as e:
        print(f"Error for student {student} and course {course}: {str(e)}")
db_cursor.close()
mydb.close()
print("fourth_values",fourth_column_values)
print("student_averages",student_averages)
message_counts = {}
for student_id1, message_count in results2:
    message_counts[student_id1] = message_count
for student_id1 in student_id:  # Assuming student_ids is the list of student_id values from your query
    message_count = message_counts.get(student_id1, 0)
    print(f"Student ID: {student_id1}, Message Count: {message_count}")

mydb = mysql.connector.connect(
  host="15.207.1.98",
  database='uatdb-ty',
  user= "uatdbmngr",
  password="%Pvji1!v*7#0",
  auth_plugin='mysql_native_password'
  
)

db_cursor = mydb.cursor()
query="SELECT performance_id,m.registration_id,exception_status,s.course_id from m_exception m INNER JOIN student_registration s on  m.registration_id=s.registration_id INNER JOIN m_courses mc ON s.course_id=mc.courses_id where (m.performance_id = 1 AND m.exception_status = 'y') OR m.performance_id IN (4, 2, 3)  and mc.status='ongoing'"
db_cursor.execute(query)
results3 = db_cursor.fetchall()
print(results3)
student_id2=[result[1] for result in results3]
not_validate=[result[1] for result in results3 if result[0]==1 and result[2]=='Y']
print("not_validate",not_validate)
student_gm=[result[1] for result in results3 if result[0]==4 and result[2]=='Y']
print("student_gm",student_gm)
student_ar=[result[1] for result in results3 if result[0] in (2,3) and result[2]=='Y']
print("student_ar",student_ar)
query="SELECT course_id,exceptions_id,status from course_exceptions where exceptions_id in (1,4)"
db_cursor.execute(query)
results4 = db_cursor.fetchall()
print(results4)
query="SELECT * from performace_rating p INNER JOIN m_courses m ON p.course_id=m.courses_id where parameters_id in (1,2,3,4,7) and active='Y' and m.status='ongoing'"
db_cursor.execute(query)
results5 = db_cursor.fetchall()
print(results5)
days=[result[7] for result in results5 if result[2] == 1]
print("days",days)
parameters_id=[result[2] for result in results5]
red_alert_percentage = [result[6] for result in results5 if result[2]==7]
rating = [result[3] for result in results5 if result[2]==4 and result[7]=='Y']
print("rating",rating)
print("red_alert_percentage",red_alert_percentage)
if not rating:
    rating=0
else:
    if rating[0] is not None:
        rating = int(rating[0])
    else:
        rating = 0
if not red_alert_percentage:
    red_alert_percentage=0.0
else:
    if red_alert_percentage[0] is not None:
        red_alert_percentage = float(red_alert_percentage[0])
    else:
        red_alert_percentage = 0.0
message_counts = {}
for student_id1, message_count in results2:
    message_counts[student_id1] = message_count
for student_id1 in student_id:  # Assuming student_ids is the list of student_id values from your query
    message_count = message_counts.get(student_id1, 0)
    print(f"Student ID: {student_id1}, Message Count: {message_count}")

for student_id,percentage in student_averages:
    message_count = message_counts.get(student_id, 0)
    print(rating,message_count)
    if percentage < red_alert_percentage and student_id not in student_id2 and days:
        print("red alert",student_id,percentage,red_alert_percentage)
        query="UPDATE student_registration SET student_status=4, system_removed_date=NOW() where registration_id=%s"
        db_cursor.execute(query, (student_id,))
        # Commit the changes
        mydb.commit()

    elif (message_count <rating) and student_id not in student_id2 and days :
        print("red alert1",student_id,message_count,rating)
        query="UPDATE student_registration SET student_status=4, system_removed_date=NOW() where registration_id=%s"
        db_cursor.execute(query, (student_id,))
        # Commit the changes
        mydb.commit()
    elif(((student_id in student_id2) and (student_id in student_ar) and (student_id in student_gm)) or (student_id in not_validate)):
        print("good performance",student_id)
    elif(student_id in student_id2) and (student_id in student_ar) and (message_count <rating):
        print("red alert2",student_id,message_count,rating)
        query="UPDATE student_registration SET student_status=4, system_removed_date=NOW() where registration_id=%s"
        db_cursor.execute(query, (student_id,))
        # Commit the changes
        mydb.commit()
    elif(student_id in student_id2 and student_id in student_gm and percentage < red_alert_percentage):
        print("red alert3",student_id,percentage,red_alert_percentage)
        query="UPDATE student_registration SET student_status=4, system_removed_date=NOW() where registration_id=%s"
        db_cursor.execute(query, (student_id,))
        # Commit the changes
        mydb.commit()
    else:
        print("good performance1",student_id)
