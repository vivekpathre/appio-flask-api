import ast
import re
import json
import psycopg2
import requests
from flask import Flask, request, jsonify

# global headers
headers = {
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json'
}

url_match = "URL"
url_add2prospects = 'URL'
url_search = "URL"
url_directdials = 'URL'

# Initializing flask app
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# connecting to remote DB
conn = psycopg2.connect(
    database="default",
    user='user',
    password='#########################',
    host='database.ondigitalocean.com',
    port='25060'
)
cur_object = conn.cursor()


api_query = """
SELECT apollo_api_key FROM auth_details WHERE ip_address = '00.000.000.000'
"""


api_key, fetch_email, result2, result3, result4, step2_result, jsn = ["" for i in range(7)]
cur_object.execute(api_query)
api_key1 = cur_object.fetchone()
api_key = api_key1[0]
print(api_key)


def fetch_details(details_list):
    global step1_result, mobile
    """function to get the details from apollo.io"""
    first_name, last_name, company_name, email_status, phone_status, pid, api_generated = details_list
    print("detailed list", details_list)

    def step1():
        global fetch_email
        """fetches emails & contacts id of requested user from apollo.io"""

        try:
            # trying to fetch the data from apollo.io
            data = {
                "api_key": api_key,
                "first_name": first_name,
                "last_name": last_name,
                "organization_name": company_name,
                "reveal_personal_emails": True
            }

            try:
                # trying to request the for data from apollo.io
                response1 = requests.request("POST",
                                             url_match,
                                             headers=headers,
                                             json=data,
                                             timeout=30
                                             )

                fetch_email = response1.json()
                fetch_email = json.loads(response1.text)
                print("fetch_email :", fetch_email)
                result = str(fetch_email).replace("'", '"')
                print("response1 :", fetch_email['person'])

                # if the person's LinkedIn url, email & title are None as
                # per first result, then set first_json as the response from
                # apollo.io in database.
                if fetch_email["person"]["linkedin_url"] is None \
                        and fetch_email["person"]["email"] is None \
                        and fetch_email["person"]["title"] is None:
                    print("Requested user data isn't available  in apollo.io")

                    query1 = """
                    INSERT INTO appio  (first_name, 
                                        last_name,
                                        company_name, 
                                        person_id, 
                                        first_json) 
                    VALUES ('{0}', '{1}', '{2}', '{3}', '{4}');""".format(
                        first_name,
                        last_name,
                        company_name,
                        fetch_email["person"]["id"],
                        result)

                    print("step1 =", query1)

                    cur_object.execute(query1)
                    conn.commit()
                    output1 = [fetch_email, 0]
                    return output1

                # if the person's LinkedIn url, email and title are all
                # together not None as per the first result, then get the
                # result and extract the required details and store it in
                # the database.
                else:
                    print(
                        "data of the requested user is available in apollo.io"
                    )

                    query2 = """INSERT INTO appio (
                                                   first_name, 
                                                   last_name, 
                                                   company_name, 
                                                   person_id, 
                                                   first_json
                                                   ) 
                    VALUES ('{0}', '{1}', '{2}', '{3}', '{4}');""".format(
                        first_name,
                        last_name,
                        company_name,
                        fetch_email["person"]["id"],
                        result
                    )
                    print("step1 =", query2)
                    cur_object.execute(query2)
                    conn.commit()
                    output1 = [fetch_email, fetch_email["person"]["id"]]
                    return output1

            except requests.exceptions.RequestException as e:
                # handle an exception to max retries to the url exceeded.
                print("2nd try block :", repr(e))
                if fetch_email['error']:
                    print('The email credits are over.')
                    return {'error': 'The email credits are over.'}
                else:
                    return {'error': 'The website is unreachable.'}

        except KeyError as e:
            # handle an exception to email credits over & other errors.
            print("1st try block :", repr(e))
            print("hi")
            if fetch_email['error']:
                return {'error': 'The email credits are over.'}
            else:
                return {'error': 'The website is unreachable.'}

    def step2(id):
        global result2, result3, result4, step2_result, jsn, results
        """fetch contact details and other important info of 
        the requested user from apollo.io"""
        print([id])
        try:
            json_data = {
                'api_key': api_key,
                'entity_ids': [id],
                'needs_direct_dial': True,
            }

            response2 = requests.post(
                url_add2prospects,
                headers=headers,
                json=json_data,
                timeout=30
            )
            result2 = json.loads(response2.text)
            print("result2 :", result2)
            print("\n")

            result_ = str(result2).replace("'", '"')
            query2 = """ UPDATE appio  
                         SET 
                            second_json = '{0}' 
                            WHERE first_name = '{1}' 
                            AND last_name = '{2}' 
                            AND company_name = '{3}'""".format(
                result_,
                first_name,
                last_name,
                company_name
            )
            print("step3 =", query2)
            cur_object.execute(query2)
            conn.commit()
            return result2

        except KeyError as e:
            print(repr(e))
            if result2["error"]:
                return {'error': 'The contact credits are over.'}
            else:
                return {'error': 'The website is unreachable.'}

    # if the api key passed is as given below then call the apollo api
    if api_generated == '############################':
        try:
            # if the user wants just email of a person and not the
            # contact details then call step1 function.
            if email_status == '0' and phone_status == '':
                print("pid :", pid)

                if pid == '':
                    step1_result = step1()
                    print("step 1 :",
                          {
                              'pid': step1_result[0]['person']['id'],
                              'email': step1_result[0]['person']['email'],
                              'personal_email': step1_result[0]['person']['personal_emails'],
                              'linkedin_url': step1_result[0]['person']['linkedin_url']
                          }
                          )

                    print(type(step1_result[0]))

                    return {
                        'pid': step1_result[0]['person']['id'],
                        'email': step1_result[0]['person']['email'],
                        'personal_email': step1_result[0]['person']['personal_emails'],
                        'linkedin_url': step1_result[0]['person']['linkedin_url']}
                else:
                    query4 = """SELECT first_json 
                                FROM appio 
                                WHERE person_id = '{}';""".format(pid)

                    print("existing data :", query4)
                    cur_object.execute(query4)

                    # display all records
                    table = cur_object.fetchall()
                    results = ''
                    for row in table:
                        results = list(row)
                        # print("results :", results[0])
                    first_json_db = ast.literal_eval(
                        results[0].replace('"', "'"))
                    print(first_json_db['person']['id'])

                    return {
                        'pid': first_json_db['person']['id'],
                        'email': first_json_db['person']['email'],
                        'personal_email': first_json_db['person']['personal_emails'],
                        'linkedin_url': first_json_db['person']['linkedin_url']
                    }

            # if the user wants email details as well as contact details of a
            # person, then call step1 as well as step2 function one after the other.
            elif email_status == '' and phone_status == '0' and pid == '':
                # if pid == '':
                id = step1()
                print("ID :", id)
                # if the data of the requested user is not available
                # then return id as 0
                if id[1] == 0:
                    print("data of the requested user is not available in apollo.io")
                    # step2_result = step2(id[1])

                    return {
                        'pid': id[0]['person']['id'],
                        'email': id[0]['person']['email'],
                        'personal_email': id[0]['person']['personal_emails'],
                        'linkedin_url': id[0]['person']['linkedin_url']
                    }
                else:
                    try:
                        mobile = ''
                        step2_result = step2(id[1])
                        for j in range(
                                0,
                                len(step2_result['contacts'][0]['phone_numbers'])
                        ):
                            if step2_result['contacts'][0]['phone_numbers'][j]['type'] == 'mobile':
                                mobile = step2_result['contacts'][0]['phone_numbers'][j]['sanitized_number']

                        return {
                            'pid': id[0]['person']['id'],
                            'email': id[0]['person']['email'],
                            'personal_email': id[0]['person']['personal_emails'],
                            'phone': mobile,
                            'linkedin_url': id[0]['person']['linkedin_url']
                        }

                    except KeyError as e:
                        print(repr(e))
                        return step2_result

            elif email_status == '' and phone_status == '0' and pid != '':
                query6 = """SELECT first_json 
                            FROM appio 
                            WHERE person_id = '{}';""".format(pid)
                print("email and phone", query6)
                cur_object.execute(query6)

                # display all records
                table = cur_object.fetchall()
                jsn3 = ''
                jsn2 = ''
                for row in table:
                    result = list(row)
                    # print("first json from database ",result)
                    jsn3 = result[0].replace("'", '"')
                    # jsn2 = result[0]
                print('jsn3 ', jsn3)
                try:
                    jsn2 = ast.literal_eval(jsn3)
                    if jsn2['person']['linkedin_url'] is None and \
                            jsn2['person']['email'] is None and \
                            jsn2['person']['title'] is None:
                        print("data is not available")

                        return {
                            'pid': jsn2['person']['id'],
                            'email': jsn2['person']['email'],
                            'personal_email': jsn2['person']['personal_emails'],
                            'phone': 'None',
                            'linkedin_url': jsn2['person']['linkedin_url']}
                    # otherwise use the person_id provided in the post request
                    # and fetch the data from apollo.io using step2.
                    else:
                        print("data is available")
                        # ID = result[3]
                        # print(ID)
                        step2_result = step2(id=pid)
                        # if step2_result['error']:
                        #     return step2_result
                        # else:
                        print('step2 ', step2_result)

                        try:
                            mobile = ''
                            for j in range(0, len(step2_result['contacts'][0]['phone_numbers'])):
                                if step2_result['contacts'][0]['phone_numbers'][j]['type'] == 'mobile':
                                    mobile = step2_result['contacts'][0]['phone_numbers'][j]['sanitized_number']

                            return {
                                'pid': jsn2['person']['id'],
                                'email': jsn2['person']['email'],
                                'personal_email': jsn2['person']['personal_emails'],
                                'phone': mobile,
                                'linkedin_url': jsn2['person']['linkedin_url']
                            }

                        except KeyError as e:
                            print(repr(e))
                            return step2_result
                except SyntaxError:
                    linkedin_url = re.findall(r'"linkedin_url": "(.*?)"', jsn3)
                    email = re.findall(r'"email": "(.*?)"', jsn3)
                    title = re.findall(r'"title": "(.*?)"', jsn3)

                    if linkedin_url == [] and email == [] and title == []:
                        print("data is not available")
                        return {
                            'pid': pid, 'email': 'None',
                            'personal_email': 'None',
                            'phone': 'None',
                            'linkedin_url': 'None'
                        }

                    # otherwise use the person_id provided in the post request
                    # and fetch the data from apollo.io using step2.
                    else:
                        print("data is available")
                        # ID = result[3]
                        # print(ID)
                        step2_result = step2(id=pid)
                        try:
                            if step2_result['error']:
                                return step2_result
                        except KeyError:
                            # step2_result = step2(id=pid)
                            step2_result_ = str(step2_result)
                            for j in range(
                                    0,
                                    len(step2_result['contacts'][0]['phone_numbers'])):
                                if step2_result['contacts'][0]['phone_numbers'][j]['type'] == 'mobile':
                                    mobile = step2_result['contacts'][0]['phone_numbers'][j]['sanitized_number']
                            print("hi, ", step2_result_)

                            personal_emails = re.findall(
                                r'"personal_emails": "(.*?)"', jsn3)

                            emails = re.findall(r'"email": "(.*?)"', jsn3)
                            # sanitized_phone = step2_result['contacts'][0]['sanitized_phone']

                            print('step2 ', step2_result)

                            return {
                                'pid': pid, 'email': emails,
                                'personal_email': personal_emails,
                                'phone': mobile,
                                'linkedin_url': linkedin_url[0]
                            }
            else:
                pass
        except KeyError as e:
            print("last try block", repr(e))
            return step1_result
    else:
        return 'Invalid api key, you dont have access to server'


@app.route('/', methods=['POST'])
def process_json():
    content_type = request.headers.get('Content-Type')
    print(request.remote_addr)
    if content_type == 'application/json':
        json_data = request.json
        details = list(json_data.values())
        op = fetch_details(details)
        print(details)
        print("returning :", type(op))
        return jsonify({'result': op})
    else:
        return 'Content-Type not supported!'


# if __name__ == "__main__":
#     app.run(debug=True, threaded=True, host="127.0.0.1" )
