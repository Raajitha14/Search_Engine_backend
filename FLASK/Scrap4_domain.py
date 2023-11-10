import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import _thread
import phonenumbers
import datetime
import pymongo
from pymongo import MongoClient
from bson.timestamp import Timestamp
import datetime as dt
from datetime import datetime


# importing module 
import traceback
client = MongoClient("localhost", 27017)
db = client["crawl_database"]
collection = db["crawled_data"]
crawled_data = db.crawled_data
admin_login=db.admin_user_info


base_path='C:/Users/user/OneDrive/Desktop/Trials/FLASK/Crawl_Results/'
crawling_queue=[]


# Function to extract links
def extract_links(soup, base_url):
    if soup is not None:
        links = []
        for anchor in soup.find_all('a', href=True):
            link = anchor['href']
            
            absolute_url = urljoin(base_url, link)
            links.append(absolute_url)
        return links
    else:
        return []

# Function to extract phone numbers
def extract_phone_numbers(text):
    phone_numbers = []
    for match in phonenumbers.PhoneNumberMatcher(text, "IN"):
        phone_number = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
        phone_numbers.append(phone_number)
    return phone_numbers

# Function to extract email IDs
def extract_email_ids(text):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    email_ids = set(re.findall(email_pattern, text))
    return email_ids

def extract_social_media_links(soup):
    # response = requests.get(website_url)
    # soup = BeautifulSoup(response.text, 'html.parser')


    social_media_links = []
    social_media_patterns = [
        r'facebook\.com',
        r'twitter\.com',
        r'linkedin\.com',
        r'instagram\.com',
        r'youtube\.com',
        r'pinterest\.com',
    
    ]

    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']
        for pattern in social_media_patterns:
            if re.search(pattern, link):
                social_media_links.append(link)

    return social_media_links

# Function to crawl the website recursively using depth-first search
def crawl_website(id,website_url,main_domain,visited_links,visited_emails,visited_phones,visited_social_media_links,ph_file,mail_file,sm_file):
    # print("website processing")
    try:
        if website_url not in visited_links and website_url.startswith(main_domain) and not website_url.endswith(".jpg") and not website_url.endswith(".pdf") and not website_url.endswith(".png") and not website_url.endswith(".jpeg") and not website_url.endswith(".PDF") and not website_url.endswith(".mp4") and not website_url.endswith(".mp3") and not website_url.endswith(".JPG") and not website_url.endswith(".JPEG") and not website_url.endswith(".PNG") and not website_url.endswith(".MP4") and not website_url.endswith(".MP3") and not website_url.endswith(".doc") and not website_url.endswith(".docx"):
            # print("processing link")
            print(website_url)
            visited_links.add(website_url)
            response = requests.get(website_url)
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')

        
            links = extract_links(soup, website_url)
            phone_numbers = extract_phone_numbers(html_content)
            email_ids = extract_email_ids(html_content)
            social_media_links = extract_social_media_links(soup)

           


            # Store the extracted data in files without overwriting("a")
            with open(mail_file, "a") as email_file:
                for email in email_ids:
                    if email not in visited_emails:
                        visited_emails.add(email)
                        email_file.write(email + "\n")

            with open(ph_file, "a") as phone_file:
                for phone in phone_numbers:
                    if phone not in visited_phones:
                        print(phone)
                        visited_phones.add(phone)
                        phone_file.write(phone + "\n")

            with open(sm_file, "a") as file:
                for Slink in social_media_links:
                    if Slink not in visited_social_media_links:
                        visited_social_media_links.add(Slink)
                        file.write(Slink + "\n")

            #print("Links")
            # Recursively crawl linked pages within the main domain
            for link in links:
                crawl_website(id,link,main_domain,visited_links,visited_emails,visited_phones,visited_social_media_links,ph_file,mail_file,sm_file)
    except Exception as e:
        print("Error occured while calling",website_url)
        print(e)






def save_toDatabase(id,website_url,email_ids_path,phone_numbers_path,social_media_path,status):
    try:
        print("in svae to db")
        data=crawled_data.find_one({"url": website_url})
        print(data)
        if data is not None:
            if email_ids_path == "":
                email_ids_path=str(data["email_file_path"])
            if phone_numbers_path =="":
                phone_numbers_path=str(data["phone no_file_path"])
            if social_media_path =="":
                social_media_path=str(data["social_media_link_path"])
        print(status)
        newValues={"$set": {"url":website_url,"crawl_id":id,"email_file_path":email_ids_path,"phone no_file_path":phone_numbers_path,"social_media_link_path":social_media_path
                   ,"status":status,"last_modified":datetime.now() }}
        print(newValues)
        crawled_data.update_one({"url": website_url},newValues)
        return True
    except Exception as e:
        traceback.print_exc()
        print(e)
        return False
def addNewRecordToDB(id,website_url,email_ids_path,phone_numbers_path,social_media_path,status):
    try:
        data=crawled_data.find_one({"url": website_url})
        if data is not None:
            return False
        data={"url":website_url} 
        data["crawl_id"]=id
        data["email_file_path"]=email_ids_path
        data["phone no_file_path"]=phone_numbers_path
        data["social_media_link_path"]=social_media_path
        data["last_modified"]=datetime.now() 
        data["status"]=status
        crawled_data.insert_one(data).inserted_id
        return True
    except Exception as e:
        print(e)
        return False



def fetchFromDatabase(input_url):
    print(input_url)
    res=crawled_data.find_one({"url": input_url})
    if res is not None:
        print(res.get("_id"))
        res["_id"]=str(res.get("_id"))
    return res



def authenticateUser(username,password):
    user_data=admin_login.find_one({"username":username})
    if user_data is None or password !=user_data["password"]:
        return False
    return True




def fetchAllRecordFromDB():
    res=crawled_data.find()
    if res is None:
        return []
    return res
    




#website_url = "http://127.0.0.1:5500/Day_2/Search/index.html"
#main_domain = website_url

#crawl_website(website_url)
def publishToQueue(queueMessage):
    crawling_queue.append(queueMessage)

def queueListener():
    print("Intialized queue")
    while(True):
        if len(crawling_queue)>0:
            queueMessage=crawling_queue.pop(0)
            id=queueMessage["id"]
            url=queueMessage["url"]
            print("before starting")
            print(save_toDatabase(id,url,"","","","CRAWLING"))
            print("after starting")
            visited_links=set()
            visited_emails=set()
            visited_phones=set()
            visited_social_media_links=set()
            ph_file=base_path+str(id)+"_phone_number.txt"
            mail_file=base_path+str(id)+"_email.txt"
            sm_file=base_path+str(id)+"_social_media.txt"
            crawl_website(id,url,url,visited_links,visited_emails,visited_phones,visited_social_media_links,ph_file,mail_file,sm_file)
            save_toDatabase(id,url,mail_file,ph_file,sm_file,"COMPLETED")




_thread.start_new_thread(queueListener,())    ### setup the workers -- give them work

