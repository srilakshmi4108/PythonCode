# import necessary libraries
# The below line imports the pandas library, which is used for data analysis.
import pandas as pd 
# This below line 6 imports the flask library, which is used for creating web applications.
#This below line 6 imports the request library, which is used to get information from the user.
# This below line 6 imports the jsonify library, which is used to convert Python objects into JSON format.
from flask import Flask,request,jsonify
import mysql.connector # This line imports the mysql.connector library, which is used to connect to MySQL databases
from mysql.connector import Error
from sqlalchemy.ext.declarative import declarative_base
import random 
from textblob import TextBlob 
import numpy as np 
from datetime import date
from datetime import datetime
import schedule
import time
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

# disable pandas warnings
pd.options.mode.chained_assignment = None
# connect to MySQL database


mydb = mysql.connector.connect(
  host="13.200.60.83",
  database='ty_data_master',
  user= "typrddbadmn",
  password="H?yOHoT0pHeq!ClZ?6RI",
  auth_plugin='mysql_native_password'
  
)



#################
# If automapping is allowed, the following code is executed:
def execute_mapping_code():
    db_cursor = mydb.cursor()
    db_cursor.execute('SELECT * FROM student_registration')
    table_rows = db_cursor.fetchall()
    print("student reg size = ", len(table_rows))
    registration = pd.DataFrame(table_rows)
    db_cursor.execute("SHOW columns FROM student_registration")
    cols = [column[0] for column in db_cursor.fetchall()]
    registration.columns = cols

    db_cursor.execute('SELECT * FROM m_configurable_parameters')
    table_rows = db_cursor.fetchall()
    print("m_configurable_parameters size = ", len(table_rows))
    m_configurable_parameters = pd.DataFrame(table_rows)
    print("Exlusion value ",m_configurable_parameters)
    db_cursor.execute("SHOW columns FROM m_configurable_parameters")
    cols = [column[0] for column in db_cursor.fetchall()]
    m_configurable_parameters.columns = cols

    db_cursor.execute('SELECT * FROM professions')
    table_rows = db_cursor.fetchall()
    professions = pd.DataFrame(table_rows)
    db_cursor.execute("SHOW columns FROM professions")
    cols = [column[0] for column in db_cursor.fetchall()]
    professions.columns = cols

    print("Exlusion value in execute mapping ",m_configurable_parameters)
    if m_configurable_parameters["automapping"][0] == 1:
        excluded_professions = list(professions.query("profession_name in ['Doctor','Yoga Teacher']")["profession_id"].values)
        excluded_professions1 = list(professions.query("profession_name in ['Student']")["profession_id"].values)

        # Get current Date
        current_date = pd.to_datetime('today').normalize()
        # Get Max Age Limit from m_configurable_parameters table
        age_limit = int(m_configurable_parameters["age"].values)
        int(m_configurable_parameters["age"].values)

        # Get applications_per_mentor_limit from m_configurable_parameters table
        applications_per_mentor_limit = int(m_configurable_parameters["mentor_applications_max_limit"].values)
        restricted_region_ids=list(m_configurable_parameters["region_id"].values)

        # Select all the students who are yet to be mapped
        query = "SELECT DISTINCT S.registration_id, S.gender_id, S.mother_tongue, S.english_communicate, S.state, S.course_id, SL.language_id, S.country, S.region_id FROM student_registration S JOIN role_mapping R ON R.registration_id = S.registration_id LEFT JOIN student_speakinglanguages SL ON SL.student_id = S.registration_id JOIN m_courses M ON S.course_id=M.courses_id WHERE is_mentor_mapped = 'N' AND R.role_id in (2,8,9,10,11) AND (SL.language_id IS NULL OR SL.language_id IS NOT NULL) AND S.student_status in (1,5) AND M.status='forthcoming'"
        cursor = mydb.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        # df_results = pd.DataFrame(results)
        # Get all unassigned student list
        unassigned = [result[0] for result in results]
        # Get Unique list of  unassigned students
        unique_unassigned = set(unassigned)
        unassigned_students = list(unique_unassigned)
        print(unassigned_students)

        # Get date_of_birth from student_registration table
        registration['date_of_birth'] = pd.to_datetime(registration['date_of_birth'])
        now = datetime.now()
        # calculate Age of today
        registration['age'] = (now - registration['date_of_birth']) / pd.Timedelta(days=365.25)
        # Select student list matching the positions like gender_id, mother_tongue, english_communicate, state, mentor_id, chief_mentor_id, region_name
        query = "SELECT DISTINCT S.registration_id, S.gender_id, R.role_id, S.mother_tongue, S.english_communicate, S.state, S.course_id, CM.chief_mentor_id, SL.language_id, S.country, S.state, S.region_id,CM.is_special_mentor from student_registration S JOIN role_mapping R ON R.registration_id = S.registration_id JOIN chief_mentor_mentor_mapping CM ON CM.mentor_id = S.registration_id LEFT JOIN student_speakinglanguages SL ON SL.student_id = S.registration_id JOIN m_courses M ON S.course_id=M.courses_id where role_id = 3 AND (SL.language_id IS NULL OR SL.language_id IS NOT NULL) AND S.student_status in (1,5) AND M.status='forthcoming'"
        cursor.execute(query)
        results1 = cursor.fetchall()
        print(results1)

        # extract mentor_id
        mentor_id = [result[0] for result in results1]
        # extract gender_data
        gender_data = [result[1] for result in results1]
        # extract role_data
        role_data = [result[2] for result in results1]
        # extract course_id
        course_id = [result[6] for result in results1]
        # extract chief_mentor_id
        chief_mentor_id = [result[8] for result in results1]
        unassigned_students_copy = unassigned_students.copy()

        assigned_students = []
        unique_mentor_id = set(mentor_id)
        unique_mentor_id_list = list(unique_mentor_id)
        print(unique_mentor_id_list)
        num_data = len(unique_mentor_id_list)
        assigned_mentors = {}

        # Get max_students_per_mentor
        max_students_per_mentor = int(applications_per_mentor_limit)
        print(max_students_per_mentor)

        len(results)
        # Execute a query to fetch the existing mappings from the database
        query = "SELECT mentor_id, COUNT(*) as student_count FROM student_mentor_chief_mentor_mapping GROUP BY mentor_id"
        cursor.execute(query)
        existing_mappings = cursor.fetchall()

        # Create a dictionary to store the mentor's current student count
        mentor_student_count = {mentor_id: student_count for mentor_id, student_count in existing_mappings}
        # The following definition assign_students_to_mentors assigns Mentors based on the conditions
        def assign_students_to_mentors(results, assigned_mentors, unassigned_students):
            unassigned_students_copy = unassigned_students[:]
            mentor_applications_count = {}
            for mentor_id in assigned_mentors.values():
                mentor_applications_count[mentor_id] = 0

            mentor_student_mapping = {}
            satisfying_students = []
            for res in results:
                region_id = res[8]
                state_name = res[4]
                country_name = res[7]
                student_id = res[0]
                student_speaking_id = res[6]
                student_row = registration.loc[registration['registration_id'] == student_id]
                age = student_row['age'].values[0]

                if (m_configurable_parameters["exclusion_Criteria"][0] == 1 and m_configurable_parameters["profession"][0] == 1 and student_row['profession_id'].item() in excluded_professions or
                        m_configurable_parameters["exclusion_Criteria"][0] == 1 and m_configurable_parameters["profession"][0] == 1 and student_row['profession_id'].item() in excluded_professions1 and
                        age < age_limit):
                    continue
                if region_id in restricted_region_ids:
                    continue
                student_gender = student_row['gender_id'].item()
                student_mother_tongue = student_row['mother_tongue'].item()
                student_english_communicate = student_row['english_communicate'].item()
                state_name = student_row['state'].item()
                course_id=student_row['course_id'].item()

                mentor_scores = {}
                for mentor_id in unique_mentor_id_list:
                    mentor_row = results1[unique_mentor_id_list.index(mentor_id)]
                    mentor_gender = mentor_row[1]
                    mentor_mother_tongue = mentor_row[3]
                    mentor_english_communicate = mentor_row[4]
                    mentor_state = mentor_row[5]
                    mentor_speaking_id = mentor_row[8]
                    mentor_region = mentor_row[11]
                    special_mentor=mentor_row[12]
                    mentor_course_id=mentor_row[6]
                    score = 0
                    if region_id == mentor_region and course_id == mentor_course_id:
                        if (student_gender == 2 and student_gender == mentor_gender) or (student_gender == 3 and mentor_gender == 2):
                            score += 3
                        elif student_gender != 2 and student_gender != 3:
                            score += 3
                        else:
                            # Exclude the student and continue to the next iteration
                            continue
                    else:
                        # Exclude the student and continue to the next iteration
                        continue

                    if (student_mother_tongue == mentor_mother_tongue or
                        student_english_communicate == mentor_english_communicate == 'Y' or student_speaking_id == mentor_speaking_id):
                        score += 1
                    else:
                        # Exclude the student and continue to the next iteration
                        continue
                    
                    mentor_scores[mentor_id] = score
                    if len(mentor_scores) > 0:
                        # At least one mentor has a score for this student
                        satisfying_students.append(student_id)

                    
                mentor_scores_sorted = sorted(mentor_scores.items(), key=lambda x: x[1], reverse=True)
                special_mentors = [mentor_row[0] for mentor_row in results1 if mentor_row[12] == 'Y']
                assigned = False
                for mentor_id, score in mentor_scores_sorted:
                    if mentor_id not in assigned_mentors.values() and mentor_id not in mentor_student_mapping.keys():
                        if mentor_student_count.get(mentor_id, 0) < max_students_per_mentor:
                            mentor_student_mapping[mentor_id] = [student_id]
                            assigned_mentors[student_id] = mentor_id
                            mentor_student_count[mentor_id] = mentor_student_count.get(mentor_id, 0) + 1
                            assigned = True
                            print(f"Assigned student {student_id} to mentor {mentor_id}")
                            break
                    elif mentor_id in mentor_student_mapping.keys() and len(mentor_student_mapping[mentor_id]) < max_students_per_mentor:
                        if mentor_student_count.get(mentor_id, 0) < max_students_per_mentor:
                            mentor_student_mapping[mentor_id].append(student_id)
                            assigned_mentors[student_id] = mentor_id
                            mentor_student_count[mentor_id] = mentor_student_count.get(mentor_id, 0) + 1
                            assigned = True
                            print(f"Assigned student {student_id} to mentor {mentor_id}")
                            break

                limit_students = []
                if not assigned:
                    satisfying_students_unique = list(set(satisfying_students))
                    print(print("satisfying_students:", satisfying_students))
                    for student_id in satisfying_students_unique:
                        if student_id not in assigned_mentors:
                            limit_students.append(student_id)
                    print("limit students:", limit_students)
                    region_dict = {result[0]: result[8] for result in results}
                    for student_id in limit_students:
                        if m_configurable_parameters["special_mentor_flag"][0] == 1:
                            print("flag satisfied")
                            special_mentors = [mentor_row[0] for mentor_row in results1 if mentor_row[12] == 'Y']
                            for mentor_id in special_mentors:
                                print("special mentor")
                                mentor_row = results1[unique_mentor_id_list.index(mentor_id)]
                                mentor_region = mentor_row[11]
                                print(region_dict.get(student_id))
                                print(mentor_region)
                                if region_dict.get(student_id) == mentor_region:
                                    print("same region")
                                    mentor_student_mapping[mentor_id] = [student_id]
                                    assigned_mentors[student_id] = mentor_id
                                    assigned = True
                                    print(f"Assigned student {student_id} to special mentor {mentor_id}")
                                    break
                # print("Unassigned students:", limit_students)
                if not assigned:
                    unassigned_students_copy.append(student_id)

            # print(f"assigned_mentors_mapping: {assigned_mentors}")
            # print(f"assigned_mentors: {assigned_mentors[student_id]}")
            # print(f"unassigned_students_copy: {unassigned_students_copy}")
            return assigned_mentors, unassigned_students_copy

        assigned_mentors, unassigned_students_copy = assign_students_to_mentors(results, assigned_mentors,
                                                                                 unassigned_students_copy)


        query = 'INSERT INTO student_mentor_chief_mentor_mapping(course_id, region_id, student_id, mentor_id, cheif_mentor_id) VALUES (%s, %s, %s, %s, %s)'

        for student_id, mentor_id in assigned_mentors.items():
            chief_mentor_id = None
            for mentor_info in results1:
                if mentor_info[0] == mentor_id:
                    course_id = mentor_info[6]
                    chief_mentor_id = mentor_info[7]
                    region_id = mentor_info[11]
                    break
            data = (course_id, region_id, student_id, mentor_id, chief_mentor_id)

            try:
                cursor.execute(query, data)
                mydb.commit()
                print("Insert successful.")
            except mysql.connector.IntegrityError as e:
                print("IntegrityError:", e)
                mydb.rollback()
                print("Insert failed.")
        
        # Insert mentor data without duplicates
        for mentor_id in unique_mentor_id_list:
            for mentor_info in results1:
                if mentor_info[0] == mentor_id:
                    course_id = mentor_info[6]
                    chief_mentor_id = mentor_info[7]
                    region_id = mentor_info[11]
                    break
            mentor_data = (course_id, region_id, mentor_id, chief_mentor_id, chief_mentor_id)
            try:
                cursor.execute(query, mentor_data)
                mydb.commit()
                # print("Insert successful.")
            except mysql.connector.IntegrityError as e:
                print("IntegrityError:", e)
                mydb.rollback()
                print("Insert failed.")
        unique_chief_mentor_ids = set()

# Retrieve unique chief mentor IDs
        for mentor_id in unique_mentor_id_list:
            for mentor_info in results1:
                if mentor_info[0] == mentor_id:
                    unique_chief_mentor_ids.add(mentor_info[7])
                    break
        print(unique_chief_mentor_ids)
        for chief_mentor_id in unique_chief_mentor_ids:
            for mentor_info in results1:
                if mentor_info[7] == chief_mentor_id:
                    course_id = mentor_info[6]
                    region_id = mentor_info[11]
                    break
            cheif_mentor_data = (course_id, region_id, chief_mentor_id, chief_mentor_id, chief_mentor_id)
            try:
                cursor.execute(query, cheif_mentor_data)
                mydb.commit()
                # print("Insert successful.")
            except mysql.connector.IntegrityError as e:
                print("IntegrityError:", e)
                mydb.rollback()
                print("Insert failed.")
        query = 'SELECT student_mapping_id,region_id,student_id, course_id, cheif_mentor_id, mentor_id FROM student_mentor_chief_mentor_mapping'
        cursor.execute(query)
        results2 = cursor.fetchall()
        print(results2)
        for result in results2:
            student_id = result[2]
            print(student_id)
            query = """UPDATE student_registration SET is_mentor_mapped='Y' WHERE registration_id=%s"""
            cursor.execute(query, (student_id,))
            # result3=cursor.fetchall()
            # print("Updates are Successful")

        mydb.commit()
        db_cursor.close()
    else:
        print("No Student Mentor mapping since Automapping is not allowed")





# =============================================================================
# schedule.every(1).minutes.do(execute_mapping_code)
# 
# # Run the scheduled code indefinitely
# while True:
#     schedule.run_pending()
#     time.sleep(1)
# 
# =============================================================================
 

@app.route('/execute')
def execute():
    

    return 'Mapping code executed successfully!'



@app.route('/execute-mapping', methods=['POST'])
def execute_mapping():
    # print("inside api call")
    
    # db_cursor = mydb.cursor()
    # print("After db cursor")


    # registration = None
    # m_configurable_parameters = None
    # professions = None
    
    
    automapping = request.json.get('automapping')
    
    execute_mapping_code()

    # db_cursor.close()
    
    return 'Mapping code executed successfully'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
