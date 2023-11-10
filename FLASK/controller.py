from flask import Flask,render_template,url_for,request,jsonify
from Scrap4_domain import *
import asyncio
import time
import random


#from flask_sqlalchemy import SQLAlchemy

app=Flask(__name__)


@app.route('/addnewurl',methods=["POST"])
def index():
    #return render_template('index.html') 
    input=request.get_json(force=True)
    print(input["url"])
    crawl_id=getCrawlerId()
    queueMessage={"id":crawl_id,"url":input["url"]}
    if(not addNewRecordToDB(crawl_id,input["url"],"","","","RECIEVED")):
        response={"message":"URL Addition Fialed","status":500}
        response=jsonify(response)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response,500
    publishToQueue(queueMessage)
    response={"message":"url added successfully", "status":200}
    response=jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response,200

def getCrawlerId():
    timeStamp=time.time()
    randomNo=random.randrange(1,1000)
    return str(timeStamp)+"_"+str(randomNo)


@app.route('/fetchData',methods=["POST"])
def fetchData():
    input=request.get_json(force=True)
    # print(input["url"])
    response=fetchFromDatabase(input["url"])
    if response is None:
        response={"message":"Invalid url","status":400}
    else:
        response={"data":response,"status":200}
    response=jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response,200

@app.route('/login',methods=['POST'])
def login():
    input=request.get_json(force=True)
    print(input["username"])
    if authenticateUser(input["username"],input["password"]):
        response = {"message": "Login successful","status":200}
        response=jsonify(response)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response,200
    else:
        response={"message":"Login failed","status":401}
        response=jsonify(response)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response,401
    


@app.route('/scrapeall',methods=['GET'])  
def scrapeAll():
    crawl_data=fetchAllRecordFromDB()
    for data in crawl_data:
        url=str(data["url"])
        print(url)
        crawl_id=getCrawlerId()
        queueMessage={"id":crawl_id,"url":url}
        print(save_toDatabase(crawl_id,url,"","","","RECIEVED"))
        publishToQueue(queueMessage)
    response = {"message": "Initiated crawling","status":200}
    response=jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response,200

@app.route('/fetchall')
def fetchAll():
    crawl_data=fetchAllRecordFromDB()
    res=[]
    for data in crawl_data:
        obj={"crawl_id":data["crawl_id"],
             "url":data["url"],
             "status":data["status"],
             "last_modified":data["last_modified"],
             "email_file_path":data["email_file_path"],
             "phone no_file_path":data["phone no_file_path"],
             "social_media_link_path":data["social_media_link_path"]}
        res.append(obj)

    response=jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response,200

    




    
   
    





if __name__=="__main__":
    app.run(debug=True)
    
