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
    count = 0
    failed_files = 0

    for root, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                with open(full_path, "r") as f:
                    file_data = json.load(f)

                if isinstance(file_data, list):
                    collection.insert_many(file_data)
                else:
                    collection.insert_one(file_data)

                count += len(file_data) if isinstance(file_data, list) else 1

            except json.JSONDecodeError as e:
                print(f"Error processing file {file}: {e}")
                failed_files += 1
                continue
            except errors.PyMongoError as e:
                print(f"Database error while processing file {file}: {e}")
                continue

    with open("count.txt", "w") as count_file:
        count_file.write(str(count))

    print(f"Total records imported: {count}")
    print(f"Files failed to process: {failed_files}")


if __name__ == "__main__":
    main()
