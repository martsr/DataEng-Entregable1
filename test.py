from dotenv import load_dotenv
import os

load_dotenv()
config = {
        'host': os.getenv("HOST"),
        'port': os.getenv("PORT"),
        'database': os.getenv("DATABASE"),
        'user': os.getenv("USER"),
        'password': os.getenv("PASSWORD")
        }
print(config)