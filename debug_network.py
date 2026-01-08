import toml
import socket
import requests
from urllib.parse import urlparse
import os

def check_dns(hostname):
    print(f"\nTesting DNS resolution for: {hostname}")
    try:
        info = socket.getaddrinfo(hostname, 443)
        print(f"✅ DNS Resolution successful: {info[0][4]}")
        return True
    except socket.gaierror as e:
        print(f"❌ DNS Resolution FAILED: {e}")
        return False

def check_connection():
    # 1. Check General Internet
    print("--- General Connectivity Check ---")
    if not check_dns("google.com"):
        print("CRITICAL: Cannot resolve google.com. You have no internet or DNS is broken.")
        return

    if not check_dns("supabase.co"):
        print("CRITICAL: Cannot resolve supabase.co. Supabase might be blocked.")
    
    # 2. Check Specific Project
    print("\n--- Project Check ---")
    secrets_path = ".streamlit/secrets.toml"
    if not os.path.exists(secrets_path):
        print(f"❌ {secrets_path} not found!")
        return

    try:
        secrets = toml.load(secrets_path)
    except Exception as e:
        pass

    supabase_config = secrets.get("supabase", {})
    url = supabase_config.get("url", "")
    
    parsed = urlparse(url)
    if parsed.hostname:
        check_dns(parsed.hostname)
    else:
        print(f"Invalid URL in secrets: {url}")

if __name__ == "__main__":
    check_connection()
