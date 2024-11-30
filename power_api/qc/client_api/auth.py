import secrets
import os
from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader

# API Key Configuration
API_KEY_NAME = "X-API-KEY"
API_KEY_FILE = "api_key.txt"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

def get_or_generate_api_key():
    """Generate and store a secure API key if it doesn't exist."""
    if not os.path.exists(API_KEY_FILE):
        api_key = secrets.token_urlsafe(32)
        with open(API_KEY_FILE, "w") as f:
            f.write(api_key)
        print(f"Generated new API key: {api_key}")
    else:
        with open(API_KEY_FILE, "r") as f:
            api_key = f.read().strip()
    return api_key

# Load or generate the API key
VALID_API_KEY = get_or_generate_api_key()

def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify the API key."""
    if api_key != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

