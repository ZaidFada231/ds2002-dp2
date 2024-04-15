import os
import json
from pymongo import MongoClient, errors


def main():
    MONGOPASS = os.getenv("MONGOPASS")
    uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
    client = MongoClient(
        uri,
        username="nmagee",
        password=MONGOPASS,
        connectTimeoutMS=200,
        retryWrites=True,
    )

    db = client["ycq2zz"]
    collection = db["Data_Project_2"]

    path = "./data"
    total_records_imported = 0
    total_failed_imports = 0
    total_corrupted_documents = 0

    for root, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                with open(full_path, "r") as f:
                    file_data = json.load(f)

                if isinstance(file_data, list):
                    result = collection.insert_many(file_data)
                    total_records_imported += len(result.inserted_ids)
                else:
                    result = collection.insert_one(file_data)
                    total_records_imported += 1 if result.inserted_id else 0

            except json.JSONDecodeError as e:
                print(f"Error processing file {file}: {e}")
                total_corrupted_documents += 1
                continue
            except errors.PyMongoError as e:
                print(f"Database error while processing file {file}: {e}")
                total_failed_imports += 1
                continue

    with open("count.txt", "w") as count_file:
        count_file.write(f"Total records imported: {total_records_imported}\n")
        count_file.write(f"Total failed imports: {total_failed_imports}\n")
        count_file.write(f"Total corrupted documents: {total_corrupted_documents}\n")

    print(f"Total records imported: {total_records_imported}")
    print(f"Total failed imports: {total_failed_imports}")
    print(f"Total corrupted documents: {total_corrupted_documents}")


if __name__ == "__main__":
    main()
