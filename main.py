#connect to mongo database
#TODO: get one programatically run query that outputs a visualization --> in python
'''
extract data from mongo --> use PyMongo to make that visualization, matplotlib, etc,
'''
from pymongo import MongoClient
from datetime import datetime
from pprint import pprint
import random as rnd
import time


#query 1 - basic
#what are the names of all the restaurants in the Bronx
result = collection.distinct("name", {"borough": "Bronx"})
print(result)

#query 2 - basic
#how many restaurants are there in each borough?

pipeline = [{"$group": {"_id": "$borough", "count": {"$sum": 1}}}]

result = list(collection.aggregate(pipeline))
print(result)

#query 3 - basic
#what is the average score for all restaurants in teh dataset?

pipeline = [{"$unwind": "$grades"}, {"$group": {"_id": None, "avg_score": {"$avg": "$grades.score"}}}]

result = list(collection.aggregate(pipeline))
print(result)

#query 4 - basic
#what are the anesm of all the Chinese restaurants in Queens that have a score of 20 or higher?

query = {"borough": "Queens", "cuisine": "Chinese", "grades.score": {"$gte": 20}}
projection = {"name": 1, "_id": 0}

result = list(collection.find(query, projection))
print(result)

#query 5 - basic
What are the top 10 cuisines in terms of the number of restaurants in the dataset?

pipeline = [{"$group": {"_id": "$cuisine", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10}]

result = list(collection.aggregate(pipeline))
print(result)

#query 6 - intermediate
#find the distinct cuising types in the restaurants collection

cuisines = db.restaurants.distinct("cuisine")
print(cuisines)

#query 7 - intermediate
# find the name, address, and grade of the restaurant with the highest grade score

result = db.restaurants.find({}, {"name": 1, "address": 1, "grades": {"$elemMatch": {"score": {"$gte": 0}}}})
result = result.sort("grades.score", pymongo.DESCENDING).limit(1)
for doc in result:
    print(doc)

#query 8 - intermediate
#find the count of restaurants that have a cuisine of Italian and are located in Manhattan borough
count = db.restaurants.count_documents({"borough": "Manhattan", "cuisine": "Italian"})
print(count)

#query 9 - hard
#what are the top 10 restaurants with the highest average grade scores for all their grades?

top_restaurants = db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        "_id": "$_id",
        "name": {"$first": "$name"},
        "average_score": {"$avg": "$grades.score"}
    }},
    {"$sort": {"average_score": -1}},
    {"$limit": 10}
])
for restaurant in top_restaurants:
    print(restaurant)

#query 10 - hard
#what are the 5 most common cuisine types in Manhattan and their respective counts?

manhattan_cuisine_counts = db.restaurants.aggregate([
    {"$match": {"borough": "Manhattan"}},
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}
])
for cuisine_count in manhattan_cuisine_counts:
    print(cuisine_count)
