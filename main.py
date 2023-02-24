# connect to mongo database
# TODO: get one programatically run query that outputs a visualization --> in python --> pref. intermediate or hard
# submit python code with just this query
'''
extract data from mongo --> use PyMongo to make that visualization, matplotlib, etc,
'''
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# TODO: export data to google doc, intermediate query #7

# 3 things you need:
# requirement and expectations of the query
# actual mongo query --> parameter, count, best grades, etc
# result of query

from pymongo import MongoClient

# connecting to the Mongo
client = MongoClient("mongodb://localhost:27017")
print("connected")
db = client.HW3

# query 1 - basic
# How many restaurants are in the collection
result = db.Restaurant.count_documents({})
# print(result)


# query 2 - basic
# what are the names of all the Chinese restaurants in Queens that have a score of 50 or higher?

query = {"borough": "Queens", "cuisine": "Chinese", "grades.score": {"$gte": 50}}
projection = {"name": 1, "_id": 0}
result = list(db.Restaurant.find(query, projection))
# print(result)

# query 3 - basic
# how many restaurants are there in each borough?
pipeline = [{"$group": {"_id": "$borough", "count": {"$sum": 1}}}]
result = list(db.Restaurant.aggregate(pipeline))
# print(result)

# query 4 - basic
# what is the average score for all restaurants in the dataset?

pipeline = [{"$unwind": "$grades"}, {"$group": {"_id": None, "avg_score": {"$avg": "$grades.score"}}}]
result = list(db.Restaurant.aggregate(pipeline))
# print(result)


# query 5 - basic
# what are the top 10 cuisines based on number of restaurats int he dataset?

pipeline = [{"$group": {"_id": "$cuisine", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10}]

result = list(db.Restaurant.aggregate(pipeline))
# print(result)

# query 6 - intermediate
# find the distinct cuising types in the restaurants collection

cuisines = db.Restaurant.distinct("cuisine")
# print(cuisines)


# query 7 - intermediate
# find the count of restaurants that have a cuisine of Italian and are located in Manhattan borough
count = db.Restaurant.count_documents({"borough": "Manhattan", "cuisine": "Italian"})
# print(count)


# query 8 - intermediate
# What are the 5 most common cuisine types in Manhattan and their respective counts?
manhattan_cuisine_counts = db.Restaurant.aggregate([
    {"$match": {"borough": "Manhattan"}},
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}
])
# for cuisine_count in manhattan_cuisine_counts:
# print(cuisine_count)


# query 9 - hard
# what are the top 10 restaurants with the highest average grade scores for all their grades and their cuisine?

top_restaurants = db.Restaurant.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        "_id": "$_id",
        "name": {"$first": "$name"},
        "average_score": {"$avg": "$grades.score"},
        "cuisine": {"$first": "$cuisine"}
    }},
    {"$sort": {"average_score": -1}},
    {"$limit": 10}
])

df_top_restaurants = pd.DataFrame(columns=["name", "cuisine", "average_score"])
for restaurant in top_restaurants:
    df_top_restaurants.loc[len(df_top_restaurants.index)] = \
        [restaurant['name'], restaurant['cuisine'], restaurant['average_score']]
    # print(restaurant)
fig = plt.figure(figsize=(10,6))

ax = sns.barplot(data=df_top_restaurants,
            y= df_top_restaurants['name'],
            x=df_top_restaurants['average_score'],
            hue=df_top_restaurants['cuisine'],
            width=.5,
            dodge=False,
            orient='h')
fig.subplots_adjust(top=.95)

plt.legend(bbox_to_anchor=(.78, 0.35), loc='upper left', title="cuisine",borderaxespad=0)

# creating the bar plot
plt.ylabel("Restaurant")
plt.xlabel("Average rating")
plt.title("Top Restaurants in Manhattan")
plt.show()

# query 10 - hard
# what are the 5 most common cuisine types in Manhattan and their respective counts?

manhattan_cuisine_counts = db.Restaurant.aggregate([
    {"$match": {"borough": "Manhattan"}},
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}
])
# for cuisine_count in manhattan_cuisine_counts:
# print(cuisine_count)
