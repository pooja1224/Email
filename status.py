import imaplib
import email
import html2text
import mysql.connector
from datetime import datetime

# status pending(halted),closed,completed
# update through sql
# without description add buisness unit code

mydb = mysql.connector.connect(host='localhost', port=3306, user='root', password='vrdella!6', database="new_status")
mycursor = mydb.cursor()

def fetch_email_details(user, password):
    imap_url = 'imap.gmail.com'

    my_mail = imaplib.IMAP4_SSL(imap_url)

    my_mail.login(user, password)

    my_mail.select('Baxster')

    _, data = my_mail.search(None, 'UNSEEN')

    mail_id_list = data[0].split()  # IDs of all emails that we want to fetch

    msgs = []

    for num in mail_id_list:
        typ, data = my_mail.fetch(num, '(RFC822)')  # RFC822 returns the whole message (BODY fetches just body)
        msgs.append(data)

    return msgs


def client_details(msgs):
    global start, end, Value, val
    html_converter = html2text.HTML2Text()
    formatted_result = {
        "client": None,
        "Comment": None,
        "clientjobid": None,
        "job_title": None,
        "job_start_date": None,
        "job_end_date": None,
        "business_Unit": None,
        "location": None,
        "Pay_Rate": None,
        "Status": None,
        "Description": None
    }

    li = []

    for msg in msgs:
        for response_part in msg:
            if type(response_part) is tuple:
                my_msg = email.message_from_bytes(response_part[1])
                my_list = []
                my_tuple = tuple()
                if my_msg.is_multipart():
                    for part in my_msg.walk():
                        # print(part.get_content_type())
                        if part.get_content_type() == 'text/plain':
                            pri = part.get_payload()

                        elif part.get_content_type() == 'text/html':
                            # Convert HTML to plain text using html2text
                            html_content = part.get_payload(decode=True).decode()
                            plain_text = html_converter.handle(html_content)

                            #id, Value, val, start, end, res = '', '', '', '', '', ''
                            # global id
                            # global Value
                            result = ""
                            id = ""
                            if "Requisition ID" in plain_text:
                                # value = plain_text.split("Requisition ID")[1].strip('\n')
                                # value = value.strip().split('\n')[0]
                                # id = value.replace("\r", "")
                                value = plain_text.split("Requisition ID")[1].strip('\n')
                                value = value.split("---")[0].strip()
                                id = value


                                # value = plain_text.split("Requisition ID")[1].strip('\n')
                                # value = value.split("---")[0].strip()
                                # id = value
                                bool_value = "SELECT EXISTS(SELECT * FROM new_baxster WHERE clientjobid = %s )"
                                mycursor.execute(bool_value,(id,))
                                result = mycursor.fetchone()[0]
                                print(result)
                                if result == 1:
                                    my_dict = {}
                                    result = {
                                        "client": None,
                                        "Comment": None,
                                        "clientjobid": None,
                                        "job_title": None,
                                        "job_start_date": None,
                                        "job_end_date": None,
                                        "business_Unit": None,
                                        "location": None,
                                        "Pay_Rate": None,
                                        "Status": None,
                                        "Description": None
                                    }
                                    my_dict["Reason"] = "not a reason"
                                    my_dict["val"] = "submitted"
                                    if "Reason" in plain_text:
                                        # value = plain_text.split("Reason")[1].strip('\n')
                                        # value = value.strip().split('\n')[0].rstrip()
                                        # my_dict["Reason"] = value
                                        value = plain_text.split("Reason")[1].strip('\n')
                                        value = value.strip().split('\n')[0].rstrip()
                                        comment = value
                                        my_dict["Reason"] = value
                                        print(value)
                                        print(my_dict["Reason"])
                                    if "halted" in my_msg['subject']:
                                        my_dict["val"] = "halted"
                                    if "closed" in my_msg['subject']:
                                        my_dict["val"] = "closed"
                                    print(my_dict["Reason"])
                                    update = "UPDATE new_baxster SET Comment = %s,Status = %s  WHERE clientjobid = %s"
                                    mycursor.execute(update, (my_dict["Reason"],my_dict["val"],id))
                                    mydb.commit()
                                    print("already existing record")
                                    break

                                elif result == 0:
                                    res,start,end = "","",""
                                    formatted_result['client'] = "baxster"
                                    formatted_result['Comment'] = "None"
                                    my_list.append(formatted_result['client'])
                                    my_list.append(formatted_result['Comment'])
                                    # data_s = ["Requisition ID", "Requisition Title", "Requisition Start Date","Requisition End Date", "Business Unit", "Location"]
                                    # keys = ["clientjobid", "job_title", "job_start_date", "job_end_date", "business_Unit","location"]

                                    # for i, j in zip(data_s, keys):
                                    #     if i in plain_text:
                                    #         value = plain_text.split(i)[1].strip('\n')
                                    #         value = value.strip().split('\n')[0]
                                    #         formatted_result[j] = value.replace("\r", "")
                                    #         my_list.append(value.replace("\r", ""))
                                    if "Requisition ID" in plain_text:
                                        value = plain_text.split("Requisition ID")[1].strip('\n')# Remove any additional unwanted sections
                                        value = value.split("---")[0].strip()
                                        formatted_result["clientjobid"] = value
                                        my_list.append(value.replace("\r", ""))

                                    if "Requisition Title" in plain_text:
                                        value = plain_text.split("Requisition Title")[1].strip('\n')
                                        value = value.strip().split('\n')[0]
                                        formatted_result["job_title"] = value
                                        my_list.append(value.replace("\r", ""))

                                    if "Requisition Start Date" in plain_text:
                                        start = plain_text.split("Requisition Start Date")[1].strip('\n')
                                        start = start.strip().split('\n')[0].rstrip()
                                        formatted_result["job_start_date"] = start
                                        my_list.append(start)
                                        start = datetime.strptime(start, "%Y-%m-%d").date()

                                    if "Requisition End Date" in plain_text:
                                        end = plain_text.split("Requisition End Date")[1].strip('\n')
                                        end = end.strip().split('\n')[0].rstrip()
                                        formatted_result["job_end_date"] = end
                                        my_list.append(end)
                                        end = datetime.strptime(end, "%Y-%m-%d").date()

                                    if "Business Unit Code" in plain_text:
                                        value = plain_text.split("Business Unit Code")[1].strip('\n')
                                        value = value.strip().split('\n')[0]
                                        formatted_result["business_Unit"] = value
                                        my_list.append(value.replace("\r", ""))

                                    if "Location" in plain_text:
                                        value = plain_text.split("Location")[1].strip('\n')
                                        value = value.strip().split('\n')[0]
                                        formatted_result["location"] = value
                                        my_list.append(value.replace("\r", ""))

                                    if "Pay Rate:" in plain_text:
                                        value = plain_text.split("Pay Rate:")[1].strip('\n')
                                        value = value.strip().split('\n')[0]
                                        formatted_result["Pay_Rate"] = value.replace("\r", "")
                                        my_list.append(value.replace("\r", ""))

                                    if "Pay rate:" in plain_text:
                                        value = plain_text.split("Pay rate:")[1].strip('\n')
                                        value = value.strip().split('\n')[0]
                                        formatted_result["Pay_Rate"] = value.replace("\r", "")
                                        my_list.append(value.replace("\r", ""))





                                    b = datetime.now().date()
                                    if start <= b and end >= b:
                                       formatted_result['Status'] = 'Pending'
                                       my_list.append(formatted_result['Status'])
                                    else:
                                        formatted_result['Status'] = 'Completed'
                                        my_list.append(formatted_result['Status'])

                                    if "Description" in plain_text:
                                        value = plain_text.split("Description")[1]
                                        value = value.strip().split('\n')
                                        st = '\n'.join(value).rstrip()
                                        su = "Requisition Start Date"
                                        re = st.split(su)

                                        res += re[0]

                                    for k in ["Site", "Business Unit", "Site Code", "Coordinator"]:
                                        site_value = plain_text.split(k)[1].strip('\n')
                                        site_value = site_value.strip().split('\n')[0]
                                        res += k + ":" + site_value + "\n"

                                    formatted_result['Description'] = res.replace("\r", "")
                                    my_list.append(res.replace("\r", ""))
                                    #print(len(my_list))
                                    my_tuple = tuple(my_list)
                                    print(formatted_result)
                                    print(li)

                                    li.append(my_tuple)
                                    print(li)
                                    stmt = "INSERT INTO new_baxster (client,Comment,clientjobid, job_title, job_start_date, job_end_date, business_Unit,location, Pay_Rate,Status,Description) VALUES (%s, %s, %s,%s, %s, %s,%s,%s,%s,%s,%s )"
                                    #mycursor.executemany(stmt,li)
                                    values = (formatted_result['client'],formatted_result['Comment'],formatted_result['clientjobid'],formatted_result['job_title'],formatted_result['job_start_date'],formatted_result['job_end_date'],formatted_result['business_Unit'],formatted_result['location'],formatted_result['Pay_Rate'],formatted_result['Status'],formatted_result['Description'])
                                    mycursor.execute(stmt,values)
                                    mydb.commit()


email_details = fetch_email_details(user='pooja@vrdella.com', password='fqsp cnki xzas tnsq')

client_details(email_details)


mydb.close()
print("Data Inserted Successfully")

