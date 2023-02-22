#connect to mongo database
#TODO: get one programatically run query that outputs a visualization --> in python
'''
extract data from mongo --> use PyMongo to make that visualization, matplotlib, etc,
'''

import pymongo

# connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Restaurants"]
col = db["Restaurants"]

from pymongo import MongoClient

# connect to the database
client = MongoClient()
db = client['restaurants']

# EASY QUERIES

# 1. Find all restaurants in a specific borough (e.g. "Bronx")
borough = "Bronx"
result = db.restaurants.find({"borough": borough})
for r in result:
    print(r)

# 2. Find all restaurants that have received a specific grade (e.g. "B")
grade = "B"
result = db.restaurants.find({"grades.grade": grade})
for r in result:
    print(r)

# 3. Find all restaurants that serve a specific cuisine (e.g. "Italian")
cuisine = "Italian"
result = db.restaurants.find({"cuisine": cuisine})
for r in result:
    print(r)

# 4. Find all restaurants that have a score above a certain threshold (e.g. 20)
threshold = 20
result = db.restaurants.find({"grades.score": {"$gt": threshold}})
for r in result:
    print(r)

# 5. Find all restaurants that are located on a specific street (e.g. "Flatbush Avenue")
street = "Flatbush Avenue"
result = db.restaurants.find({"address.street": street})
for r in result:
    print(r)

# INTERMEDIATE QUERIES

# 6. Find the top 5 restaurants with the highest average score
result = db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {"_id": "$_id", "avg_score": {"$avg": "$grades.score"}}},
    {"$sort": {"avg_score": -1}},
    {"$limit": 5}
])
for r in result:
    restaurant = db.restaurants.find_one({"_id": r["_id"]})
    print(restaurant)

# 7. Find the borough with the highest number of restaurants
result = db.restaurants.aggregate([
    {"$group": {"_id": "$borough", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])
for r in result:
    print("Borough with highest number of restaurants:", r["_id"])

# 8. Find the number of restaurants in each borough
result = db.restaurants.aggregate([
    {"$group": {"_id": "$borough", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
])
for r in result:
    print("Borough:", r["_id"], "Count:", r["count"])

# HARD QUERIES

# 9. Find the restaurant with the highest number of "A" grades
result = db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$match": {"grades.grade": "A"}},
    {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])
for r in result:
    restaurant = db.restaurants.find_one({"_id": r["_id"]})
    print(restaurant)

# 10. Find the restaurant with the highest number of "C" or "D" grades and a score below 10
result = db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$match": {"$and": [{"grades.grade": {"$in": ["C", "D"]}}, {"grades.score": {"$lt": 10}}]}},
    {"$group": {"_id": "$name", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])

