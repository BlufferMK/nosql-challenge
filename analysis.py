# Analysis of NoSQL setup

from pymongo import MongoClient
import pandas as pd
from pprint import pprint

# Create an instance of MongoClient
mongo = MongoClient(port=27017)

# assign the uk_food database to a variable name
db = mongo['uk_food']

# review the collections in our database
print(db.list_collection_names())

# assign the collection to a variable named 'establishments'
establishments = db['establishments']

# Find the establishments with a hygiene score of 20
query = {'scores.Hygiene': 20}

# Use count_documents to display the number of documents in the result
doc_count = db.establishments.count_documents(query)
print(doc_count)

# Display the first document in the results using pprint
results = establishments.find(query)

pprint(results[0])

# Convert the result to a Pandas DataFrame
list_result = list(results)

df_hygiene_20 = pd.DataFrame(list_result)

# Display the number of rows in the DataFrame
row_count = len(df_hygiene_20)
print(row_count)

# Display the first 10 rows of the DataFrame
df_hygiene_20.head(10)


# Find the establishments with London as the Local Authority and has a RatingValue greater than or equal to 4.

query = {"$and": [{'LocalAuthorityName':{'$regex':'London'}}, 
                          {'RatingValue':{'$gte':4}}]}
        
results = establishments.find(query)

# The Local Authority Name for London is "City of London Corporation".  $regex was useful for finding these.

# Use count_documents to display the number of documents in the result
doc_count = db.establishments.count_documents(query)
print(doc_count)

# Display the first document in the results using pprint
pprint(results[0])

# Convert the result to a Pandas DataFrame
list_result = list(results)

df_London_gte4 = pd.DataFrame(list_result)

# Display the number of rows in the DataFrame
row_count = len(df_London_gte4)
print(row_count)

# Display the first 10 rows of the DataFrame
df_London_gte4.head(10)


# What are the top 5 establishments with a `RatingValue` rating value of 5, sorted by lowest hygiene score, 
# nearest to the new restaurant added, "Penang Flavours"?

# Search within 0.01 degree on either side of the latitude and longitude.
# Rating value must equal 5
# Sort by hygiene score

degree_search = 0.01
query = {"BusinessName":"Penang Flavours"}
fields = {'geocode.latitude':1,'geocode.longitude':1}
results = establishments.find(query,fields)

latitude = results[0]['geocode']['latitude']
longitude = results[0]['geocode']['longitude']

query = {"$and": [{'geocode.latitude':{'$gte':(latitude - degree_search)}}, 
                          {'geocode.latitude':{'$lte':(latitude + degree_search)}},
                          {'geocode.longitude':{'$gte':(longitude - degree_search)}},
                          {'geocode.longitude':{'$lte':(longitude + degree_search)}},
                          {'RatingValue':5}]}

sort = [('scores.Hygiene',1)]
limit = 5

results = establishments.find(query).sort(sort).limit(limit)
# Print the results
pprint(results[0])

# Convert result to Pandas DataFrame
list_result = list(results)

df_5_by_Penang = pd.DataFrame(list_result)

df_5_by_Penang.head()


# How many establishments in each Local Authority area have a hygiene score of 0?

# Create a pipeline that:
# Matches establishments with a hygiene score of 0
match_query = {'$match': {'scores.Hygiene':{'$lt': 1}}}

# Groups the matches by Local Authority
group_query = {'$group': {'_id': "$LocalAuthorityName",'restaurants with 0 hygiene score': {'$sum': 1}}}

# Sorts the matches from highest to lowest
sort_values = {'$sort': {'restaurants with 0 hygiene score': -1}}

pipeline = [match_query, group_query, sort_values]
results = list(establishments.aggregate(pipeline))

# Print the number of documents in the result
print("Number of Local Authorities", len(results))

# Print the first 10 results
pprint(results[0:10])

# Convert the result to a Pandas DataFrame
result_df = pd.DataFrame(results)

# Display the number of rows in the DataFrame
print("Rows in DataFrame: ", len(result_df))

# Display the first 10 rows of the DataFrame
result_df.head(10)


