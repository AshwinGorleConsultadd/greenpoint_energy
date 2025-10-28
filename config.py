import os
from dotenv import load_dotenv

# Read from environment; do not hardcode secrets in code
load_dotenv()  # loads variables from a .env file if present
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")



