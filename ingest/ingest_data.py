import pandas as pd
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "goodreadsDB"

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

# File to collection and unique key mapping
FILES = {
    "books.csv": ("books", "book_id"),
    "tags.csv": ("tags", "tag_id"),
    "ratings.csv": ("ratings", ["user_id", "book_id"]),
    "book_tags.csv": ("book_tags", ["goodreads_book_id", "tag_id"]),
    "to_read.csv": ("to_read", ["user_id", "book_id"]),
}

# Index definitions per collection
INDEXES = {
    "books": [
        [("title", 1), ("authors", 1)],
        [("average_rating", -1)],
        [("book_id", 1)],
    ],
    "ratings": [
        [("book_id", 1)],
        [("user_id", 1), ("book_id", 1)],
    ],
    "tags": [
        [("tag_id", 1)],
        [("tag_name", 1)],
    ],
    "book_tags": [
        [("tag_id", 1)],
        [("goodreads_book_id", 1)],
    ],
    "to_read": [
        [("user_id", 1), ("book_id", 1)],
    ],
}

# Function to import CSV data into MongoDB
async def import_csv_to_mongo(file_path, collection_name, unique_keys):
    df = pd.read_csv(file_path)
    records = df.to_dict(orient="records")
    collection = db[collection_name]

    for record in records:
        if isinstance(unique_keys, list):
            filt = {k: record[k] for k in unique_keys}
        else:
            filt = {unique_keys: record[unique_keys]}

        # Upsert ensures no duplicates
        await collection.update_one(filt, {"$set": record}, upsert=True)

    print(f"Processed {len(records)} records into '{collection_name}'")


async def create_indexes():
    for coll_name, index_list in INDEXES.items():
        coll = db[coll_name]
        for keys in index_list:
            # Unique index if explicitly required
            unique = False
            if coll_name in ["ratings", "to_read"] and len(keys) > 1:
                unique = True
            await coll.create_index(keys, unique=unique)
        print(f"Indexes created for '{coll_name}'")

# Main execution function
async def main():
    for file, (collection, keys) in FILES.items():
        await import_csv_to_mongo(file, collection, keys)

    await create_indexes()

    client.close()


# Entry point
if __name__ == "__main__":
    asyncio.run(main())
