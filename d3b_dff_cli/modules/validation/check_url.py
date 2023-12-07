import argparse
import requests

def is_valid_link(url):
    try:
        response = requests.head(url)
        return response.status_code // 100 == 2
    except requests.ConnectionError:
        return False

def main(args):
    for url_to_check in args.urls:
        # Check if the link starts with "http://" or "https://"
        if not url_to_check.startswith(("http://", "https://")):
            print(f"Error: The link {url_to_check} must start with 'http://' or 'https://'.")
            continue
        
        if not is_valid_link(url_to_check):
            print(f"Error: Invalid or can't access link: {url_to_check}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check the validity of one or more URLs.")
    parser.add_argument("urls", nargs="+", help="One or more URLs to check.")
    args = parser.parse_args()
    main(args)
