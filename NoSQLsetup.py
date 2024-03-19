# %% [markdown]
# # Eat Safe, Love

# %% [markdown]
# ## Part 1: Database and Jupyter Notebook Set Up

# %% [markdown]
# Import the data provided in the `establishments.json` file from your Terminal. Name the database `uk_food` and the collection `establishments`.
# 
# Within this markdown cell, copy the line of text you used to import the data from your Terminal. This way, future analysts will be able to repeat your process.
# 
# e.g.: Import the dataset with  
# 
# 'mongoimport --type json -d uk_food -c establishments --drop --jsonArray establishments.json'
# 

# %%
# Import dependencies
from pymongo import MongoClient
from pprint import pprint

# %%
# Create an instance of MongoClient
mongo = MongoClient(port=27017)

# %%
# confirm that our new database was created

print(mongo.list_database_names())

# %%
# assign the uk_food database to a variable name
db = mongo.uk_food


# %%
# review the collections in our new database
print(db.list_collection_names())

# %%
# review the collections in our new database
limit = 5
records = db.establishments.find().limit(limit)

pprint(list(records))



# %%
# review a document in the establishments collection
records = db.establishments.find_one({'AddressLine3':'London'})
pprint(records)

# %%
# assign the collection to a variable
establishments = db['establishments']

# %% [markdown]
# ## Part 2: Update the Database

# %% [markdown]
# 1. An exciting new halal restaurant just opened in Greenwich, but hasn't been rated yet. The magazine has asked you to include it in your analysis. Add the following restaurant "Penang Flavours" to the database.

# %%
# Create a dictionary for the new restaurant data
Penang = {
    "BusinessName":"Penang Flavours",
    "BusinessType":"Restaurant/Cafe/Canteen",
    "BusinessTypeID":"",
    "AddressLine1":"Penang Flavours",
    "AddressLine2":"146A Plumstead Rd",
    "AddressLine3":"London",
    "AddressLine4":"",
    "PostCode":"SE18 7DY",
    "Phone":"",
    "LocalAuthorityCode":"511",
    "LocalAuthorityName":"Greenwich",
    "LocalAuthorityWebSite":"http://www.royalgreenwich.gov.uk",
    "LocalAuthorityEmailAddress":"health@royalgreenwich.gov.uk",
    "scores":{
        "Hygiene":"",
        "Structural":"",
        "ConfidenceInManagement":""
    },
    "SchemeType":"FHRS",
    "geocode":{
        "longitude":"0.08384000",
        "latitude":"51.49014200"
    },
    "RightToReply":"",
    "Distance":4623.9723280747176,
    "NewRatingPending":True
}


# %%
# Insert the new restaurant into the collection
db.establishments.insert_one(Penang)

# %%
# Check that the new restaurant was inserted
query = {"BusinessName":"Penang Flavours"} 
results = establishments.find(query)
for result in results:
    print(result)

# %% [markdown]
# 2. Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the `BusinessTypeID` and `BusinessType` fields.

# %%
# Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the BusinessTypeID and BusinessType fields
query = {"BusinessTypeID": {"$eq" :1}}
fields = {"BusinessType":1,"BusinessTypeID": 1}
limit = 5

results = establishments.find(query,fields).limit(limit)


pprint(list(results))


# %% [markdown]
# 3. Update the new restaurant with the `BusinessTypeID` you found.

# %%
# Update the new restaurant with the correct BusinessTypeID
filter = {"BusinessName":"Penang Flavours"}
newvalues = {"$set": {"BusinessTypeID": 1 }
                                            }

establishments.update_one(filter, newvalues)

# %%
# Confirm that the new restaurant was updated
query = {"BusinessName":"Penang Flavours"} 
results = establishments.find(query)
for result in results:
    print(result)

# %% [markdown]
# 4. The magazine is not interested in any establishments in Dover, so check how many documents contain the Dover Local Authority. Then, remove any establishments within the Dover Local Authority from the database, and check the number of documents to ensure they were deleted.

# %%
# Find how many documents have LocalAuthorityName as "Dover"
query = {'LocalAuthorityName': 'Dover'}

doc_count = db.establishments.count_documents(query)

print(doc_count)

# %%
# Delete all documents where LocalAuthorityName is "Dover"
db.establishments.delete_many(query)

# %%
# Check if any remaining documents include Dover
results = establishments.find(query)

pprint(list(results))

# %%
# Check that other documents remain with 'find_one'
records = db.establishments.find_one({'AddressLine3':'London'})
pprint(records)

# %% [markdown]
# 5. Some of the number values are stored as strings, when they should be stored as numbers.

# %% [markdown]
# Use `update_many` to convert `latitude` and `longitude` to decimal numbers.

# %%
# Change the data type from String to Decimal for longitude and latitude
new_values = [
    {'$set':{
        'geocode.latitude':
        {'$toDouble':"$geocode.latitude"}}}]
db.establishments.update_many({}, new_values)

new_values = [
    {'$set':{
        'geocode.longitude':
        {'$toDouble':"$geocode.longitude"}}}]
db.establishments.update_many({}, new_values)

# %% [markdown]
# Use `update_many` to convert `RatingValue` to integer numbers.

# %%
# Set non 1-5 Rating Values to Null
non_ratings = ["AwaitingInspection", "Awaiting Inspection", "AwaitingPublication", "Pass", "Exempt"]
establishments.update_many({"RatingValue": {"$in": non_ratings}}, [ {'$set':{ "RatingValue" : None}} ])

# %%
# Change the data type from String to Integer for RatingValue
new_values = [
    {'$set':{
        'RatingValue':
        {'$toInt':"$RatingValue"}}}]
db.establishments.update_many({}, new_values)

# %%
# Check that the coordinates and rating value are now numbers
records = db.establishments.find_one()
pprint(records)


