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


try:
    mydb = mysql.connector.connect(
        host="15.207.1.98",
        database='uatdb-ty',
        user="uatdbmngr",
        password="%Pvji1!v*7#0",
        auth_plugin='mysql_native_password'
    )
except Error as e:
    print("MySQL connection error:", e)
    print("Retrying in 5 seconds...")
    time.sleep(5)

#################
# If automapping is allowed, the following code is executed:
def execute_mapping_code(chief_mentor):
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
    try:
        chief_mentor_config = m_configurable_parameters[
            (m_configurable_parameters['cheif_mentor_id'] == chief_mentor)].iloc[0]
    except IndexError:
        # Handle the case when no matching rows are found in the DataFrame
        error_message = "No matching configuration found for chief mentor and region."
        print(error_message)
        return jsonify({"error": error_message}), 500        
    
    if chief_mentor_config["automapping"] == 1:
        excluded_professions = list(professions.query("profession_name in ['Doctor','Yoga Teacher']")["profession_id"].values)
        excluded_professions1 = list(professions.query("profession_name in ['Student']")["profession_id"].values)

        # Get current Date
        current_date = pd.to_datetime('today').normalize()
        # Get Max Age Limit from m_configurable_parameters table
        age_limit = int(chief_mentor_config["age"])
        int(chief_mentor_config["age"])

        # Get applications_per_mentor_limit from m_configurable_parameters table
        applications_per_mentor_limit = int(chief_mentor_config["mentor_applications_max_limit"])
        # restricted_region_ids=chief_mentor_config["region_id"]

        # Select all the students who are yet to be mapped
        query = "SELECT DISTINCT S.registration_id, S.gender_id, S.mother_tongue, S.english_communicate, S.state, S.course_id, GROUP_CONCAT(DISTINCT SL.language_id) AS language_ids, S.country, S.region_id,ML.language_id as mother_tounge_id FROM student_registration S JOIN role_mapping R ON R.registration_id = S.registration_id LEFT JOIN student_speakinglanguages SL ON SL.student_id = S.registration_id JOIN m_courses M ON S.course_id=M.courses_id JOIN m_languages ML ON ML.language_name=S.mother_tongue WHERE is_mentor_mapped = 'N' AND R.role_id in (2,8,9,10,11) AND (SL.language_id IS NULL OR SL.language_id IS NOT NULL) AND S.student_status in (1,5) AND M.status!='past' AND NOT EXISTS (SELECT 1 FROM role_mapping R2 WHERE R2.registration_id = S.registration_id AND R2.role_id IN (1, 3, 4)) GROUP BY S.registration_id,S.gender_id, S.mother_tongue, S.english_communicate, S.state, S.course_id,S.country, S.region_id,ML.language_id ORDER BY registration_id"
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
        query = "SELECT DISTINCT S.registration_id, S.gender_id, R.role_id, S.mother_tongue, S.english_communicate, S.state, S.course_id, CM.chief_mentor_id, GROUP_CONCAT(DISTINCT SL.language_id) AS language_ids, S.country, S.state, S.region_id,CM.is_special_mentor,ML.language_id as mother_tounge_id from student_registration S JOIN role_mapping R ON R.registration_id = S.registration_id JOIN chief_mentor_mentor_mapping CM ON CM.mentor_id = S.registration_id LEFT JOIN student_speakinglanguages SL ON SL.student_id = S.registration_id JOIN m_courses M ON S.course_id=M.courses_id JOIN m_languages ML ON ML.language_name=S.mother_tongue WHERE (SL.language_id IS NULL OR SL.language_id IS NOT NULL) AND S.student_status IN (1, 5) AND M.status != 'past' and R.status_id=1 AND CM.chief_mentor_id =%s GROUP BY S.registration_id, S.gender_id, R.role_id, S.mother_tongue, S.english_communicate, S.state, S.course_id, CM.chief_mentor_id,S.country, S.state, S.region_id, CM.is_special_mentor,ML.language_id HAVING SUM(CASE WHEN R.role_id = 3 THEN 1 ELSE 0 END) > 0 AND SUM(CASE WHEN R.role_id <> 3 THEN 1 ELSE 0 END) = 0 AND NOT EXISTS (SELECT 1 FROM role_mapping R2 WHERE R2.registration_id = S.registration_id AND R2.role_id IN (1,4))  ORDER BY registration_id"
        cursor.execute(query, (chief_mentor,))
        results1 = cursor.fetchall()
        print(results1)
        query = "SELECT DISTINCT chief_mentor_id,mentor_id,CM.region_id,SR.gender_id from ty_data_master.chief_mentor_mentor_mapping CM JOIN student_registration SR ON CM.mentor_id = SR.registration_id WHERE CM.region_id IN (SELECT region_id FROM ty_data_master.chief_mentor_mentor_mapping GROUP BY region_id HAVING COUNT(DISTINCT chief_mentor_id) > 1)"
        cursor.execute(query)
        results5 = cursor.fetchall()
        cheif_mentor=[result[0] for result in results5]
        CM_region_id=[result[2] for result in results5]
        mentor=[result[1] for result in results5]
        mentor_list = list(mentor)
        print(mentor_list)
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
        # Execute a query to fetch the existing mappings from the database
        query = "SELECT mentor_id, COUNT(*) as student_count FROM student_mentor_chief_mentor_mapping WHERE mentor_id != %s GROUP BY mentor_id ORDER BY student_count ASC"
        cursor.execute(query, (chief_mentor,))
        existing_mappings = cursor.fetchall()
        print("existing mappings =",existing_mappings)
        query = "SELECT DISTINCT S.registration_id FROM student_registration S JOIN region_chief_mentor_mapping CM ON S.registration_id = CM.cheif_mentor_id WHERE CM.region_id IN (SELECT region_id FROM region_chief_mentor_mapping GROUP BY region_id HAVING COUNT(DISTINCT cheif_mentor_id) > 1) AND S.course_id=CM.course_id"
        cursor.execute(query)
        results6 = cursor.fetchall()
        double_mentors=[result[0] for result in results6]
        print("double_mentors",double_mentors)
        print(chief_mentor)
        query="SELECT region_id from region_chief_mentor_mapping where cheif_mentor_id=%s"
        cursor.execute(query,(chief_mentor,))
        results7 = cursor.fetchall()
        chief_mentor_region_ids=[result[0] for result in results7]
        print(chief_mentor_region_ids)
        # Filter students who belong to either of the regions of the chief mentor
        students_in_chief_mentor_region = [student for student in results if student[8] in chief_mentor_region_ids]
        print(students_in_chief_mentor_region)
        

        # Create a dictionary to store the mentor's current student count
        mentor_student_count = {mentor_id: student_count for mentor_id, student_count in existing_mappings}
        student_region_mapping = {}
        # The following definition assign_students_to_mentors assigns Mentors based on the conditions
        def assign_students_to_mentors(results, assigned_mentors, unassigned_students, chief_mentor):
            unassigned_students_copy = unassigned_students[:]
            mentor_applications_count = {}
            for mentor_id in assigned_mentors.values():
                mentor_applications_count[mentor_id] = 0
            mentor_student_mapping = {}
            satisfying_students = []
            for res in students_in_chief_mentor_region:
                region_id = res[8]
                state_name = res[4]
                country_name = res[7]
                student_id = res[0]
                student_speaking_id = res[6]
                student_mother_tongue_id = res[9]
                student_row = registration.loc[registration['registration_id'] == student_id]
                student_region_mapping[student_id] = region_id
                age = student_row['age'].values[0]
        
                if (chief_mentor_config["exclusion_Criteria"] == 1 and chief_mentor_config["profession"] == 1 and student_row['profession_id'].item() in excluded_professions or
                        chief_mentor_config["exclusion_Criteria"] == 1 and chief_mentor_config["profession"] == 1 and student_row['profession_id'].item() in excluded_professions1 or
                        age < age_limit):
                    continue
                student_gender = student_row['gender_id'].item()
                student_mother_tongue = student_row['mother_tongue'].item()
                student_english_communicate = student_row['english_communicate'].item()
                state_name = student_row['state'].item()
                course_id = student_row['course_id'].item()
        
                student_mentor_scores = {}
                mentor_list_to_use = list(unique_mentor_id)
                sorted_mentor_list=[]
                for mentor_id in unique_mentor_id_list:
                    if mentor_id not in mentor_student_count:
                        mentor_student_count[mentor_id] = 0
                filtered_mentor_student_count = {mentor_id: count for mentor_id, count in mentor_student_count.items() if mentor_id in unique_mentor_id_list}
                print("mentor_student_count=",filtered_mentor_student_count)
                sorted_mentor_assigned_count = dict(sorted(filtered_mentor_student_count.items(), key=lambda item: item[1]))
                print("sorted_mentor_assigned_count=",sorted_mentor_assigned_count)
                sorted_mentor_list = list(sorted_mentor_assigned_count.keys())
                print("Sorted Mentor List:", sorted_mentor_list)

                def validate_mentor(mentor_list_to_use):
                    check_other_scenarios = False
                    best_mentor_id = None
                    best_score = 0
                    matched_mentor_id_list = []
                    for mentor_id in mentor_list_to_use:
                        print("unique_mentor_id_list",unique_mentor_id_list)
                        mentor_row = next(row for row in results1 if row[0] == mentor_id)          
                        mentor_gender = mentor_row[1]
                        mentor_mother_tongue = mentor_row[3]
                        mentor_english_communicate = mentor_row[4]
                        mentor_state = mentor_row[5]
                        mentor_speaking_id = mentor_row[8]
                        mentor_region = mentor_row[11]
                        special_mentor=mentor_row[12]
                        mentor_course_id=mentor_row[6]
                        mentor_mother_tongue_id=mentor_row[13]
                        score = 0
                        if course_id == mentor_course_id:
                            if chief_mentor in double_mentors:
                                if (student_gender == mentor_gender) or (student_gender == 3 and mentor_gender == 2) or (student_gender == 2 and mentor_gender == 3):
                                    score += 3
                                else:
                                    continue
            
                            else:
                                if (student_gender == 2 and student_gender == mentor_gender) or (student_gender == 3 and mentor_gender == 2) or (student_gender == 2 and mentor_gender == 3):
                                    score += 3
                                elif student_gender != 2 and student_gender != 3:
                                    score += 3
                                else:
                                    continue
                                
                            check_other_scenarios = True
                            if student_mother_tongue == mentor_mother_tongue:
                                best_mentor_id = mentor_id
                                print("mother toungue matched ",student_id,mentor_id)
                                break
                            else:
                                print("mother tounge failed",student_id,mentor_id)
                                matched_mentor_id_list.append(mentor_id)
                                continue
                    if check_other_scenarios and not best_mentor_id:
                        print("not best mentor ", student_id)
                        for mentor_id in matched_mentor_id_list:
                            mentor_row = next(row for row in results1 if row[0] == mentor_id)
                            mentor_speaking_ids = []
                            student_speaking_ids=[]
                            print("mentor_mother and mentor speaking id's=",mentor_id,mentor_row[13],mentor_speaking_ids)
                            print("student mother and speaking id's",student_id,student_speaking_ids,student_mother_tongue_id)
                            
                            if mentor_row[8]:  # Check if the string is not empty
                                mentor_speaking_ids = [int(id) for id in mentor_row[8].split(',')] 
                            else:
                                print("mentor speaking id's are none")
                            if res[6]:                           
                                student_speaking_ids=[int(id1) for id1 in res[6].split(',')]
                            else:
                                print("student speaking id's are none")
                            if student_english_communicate == mentor_row[4] == 'Y':
                                best_mentor_id = mentor_id
                                print("english matched",student_id,mentor_id)
                                assigned = True
                                break
                            elif any(id in mentor_speaking_ids for id in student_speaking_ids):
                                best_mentor_id = mentor_id
                                print("other language matched",student_id,mentor_id)
                                assigned = True
                                break                    
                            
                            elif mentor_row[13] in student_speaking_ids or student_mother_tongue_id in mentor_speaking_ids:
                                best_mentor_id = mentor_id
                                print("cross matched",student_id,mentor_id)
                                assigned = True
                                break 
                            else:
                                continue
                    return best_mentor_id     
                best_mentor_id=validate_mentor(sorted_mentor_list)
                mentor_limit = mentor_student_count.get(best_mentor_id, 0)
                
                if best_mentor_id :
                    if mentor_limit < max_students_per_mentor:

                        if best_mentor_id not in mentor_student_mapping:
                            mentor_student_mapping[best_mentor_id] = [student_id]
                        else:
                            mentor_student_mapping[best_mentor_id].append(student_id)

                        assigned_mentors[student_id] = best_mentor_id
                        mentor_student_count[best_mentor_id] = mentor_limit + 1  # Increment mentor limit
                        satisfying_students.append((student_id, best_mentor_id))
                        print(f"Assigned student {student_id} to mentor {best_mentor_id} with best score")
                        
                    else:
                        special_mentors = [mentor_row[0] for mentor_row in results1 if mentor_row[12] == 'Y']
                        print(mentor_student_count)
                        for mentor_id in special_mentors:
                            if mentor_id not in mentor_student_count:
                                mentor_student_count[mentor_id] = 0
                        filtered_smentor_student_count = {mentor_id: count for mentor_id, count in mentor_student_count.items() if mentor_id in special_mentors}
                        sorted_smentor_assigned_count = dict(sorted(filtered_smentor_student_count.items(), key=lambda item: item[1]))
                        print("sorted_smentor_assigned_count=",sorted_smentor_assigned_count)
                        sorted_smentor_list = list(sorted_smentor_assigned_count.keys())
                        print("Sorted sMentor List:", sorted_smentor_list)
                        if sorted_smentor_list:
                            print("Checking for special mentor")
                            best_mentor_id = validate_mentor(sorted_smentor_list)
                            if best_mentor_id is not None:
                                print("Found valid best mentor:", best_mentor_id)
                                if best_mentor_id not in mentor_student_mapping:
                                    mentor_student_mapping[best_mentor_id] = [student_id]
                                else:
                                    mentor_student_mapping[best_mentor_id].append(student_id)

                                assigned_mentors[student_id] = best_mentor_id
                                mentor_student_count[best_mentor_id] =mentor_student_count[best_mentor_id] +1  # Increment mentor limit
                                satisfying_students.append((student_id, best_mentor_id))
                                print(f"Assigned student {student_id} to special mentor {best_mentor_id} with score")
                            else:
                                print("No valid best mentor found for special mentors")
                        else:
                            print("No special mentors found")

                            
                else:
                    unassigned_students_copy.append(student_id)
            return assigned_mentors, unassigned_students_copy

        assigned_mentors, unassigned_students_copy = assign_students_to_mentors(results, assigned_mentors,
                                                                         unassigned_students_copy, chief_mentor)


        query = 'INSERT INTO student_mentor_chief_mentor_mapping(course_id, region_id, student_id, mentor_id, cheif_mentor_id) VALUES (%s, %s, %s, %s, %s)'
        
        print(unique_mentor_id_list)
        # Insert mentor data without duplicates
        for mentor_id in unique_mentor_id_list:
            for mentor_info in results1:
                if mentor_info[0] == mentor_id:
                    course_id = mentor_info[6]
                    region_id = chief_mentor_region_ids[0]
                    break
            mentor_data = (course_id, region_id, mentor_id, chief_mentor, chief_mentor)
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
        
        for mentor_info in results1:
            if mentor_info[7] == chief_mentor:
                course_id = mentor_info[6]
                region_id = chief_mentor_region_ids[0]
                break
        cheif_mentor_data = (course_id, region_id, chief_mentor, chief_mentor, chief_mentor)
        try:
            cursor.execute(query, cheif_mentor_data)
            mydb.commit()
            # print("Insert successful.")
        except mysql.connector.IntegrityError as e:
            print("IntegrityError:", e)
            mydb.rollback()
            print("Insert failed.")
        print("assigned mentors", assigned_mentors)
        for student_id, mentor_id in assigned_mentors.items():
            for mentor_info in results1:
                if mentor_info[0] == mentor_id:
                    course_id = mentor_info[6]
                    print(mentor_info[0])
                    region_id = student_region_mapping.get(student_id)
                    break
            data = (course_id, region_id, student_id, mentor_id, chief_mentor)
            print("Data:", data)  # Print the data to be inserted (for debugging)

            try:
                cursor.execute(query, data)
                mydb.commit()
                print("Insert successful.")
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





@app.route('/execute-mapping', methods=['POST'])
def execute_mapping(): 
    
    automapping = request.json.get('automapping')
    chief_mentor= request.json.get('chief_mentor_id')
    execute_mapping_code(chief_mentor)
    
    return 'Mapping code executed successfully'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
