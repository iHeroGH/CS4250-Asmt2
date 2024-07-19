#-------------------------------------------------------------------------
# AUTHOR: George Matta
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Backend functionalty of mongo DB connection
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3hr
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import pymongo
import datetime
import string

def connectDataBase():
    DB_NAME = "web_search"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = pymongo.MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db
    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    counter = {}
    for word in docText.translate(str.maketrans('', '', string.punctuation)).split(" "):
        word = word.lower()
        if word in counter:
            counter[word] += 1
        else:
            counter[word] = 1

    term_obs = [
        {
            "term": term,
            "count": counter[term],
            "num_char": len(term)
        }
        for term in counter
    ]

    doc = {
        "doc": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": sum([term_ob['num_char'] for term_ob in term_obs]),
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": term_obs
    }

    col.insert_one(doc)

def deleteDocument(col, docId):
    col.delete_one({"doc": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    deleteDocument(col, docId)
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    index = {
        term_ob['term']: f"{doc['title']}:{term_ob['count']}"
        for doc in col.find()
        for term_ob in doc['terms']
    }

    return dict(sorted(index.items()))
