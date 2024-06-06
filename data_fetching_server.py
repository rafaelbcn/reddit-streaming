# data_fetching_server.py
import os
import dotenv
import requests
import socket
import json
import threading

# Load environment variables
env_file = 'creds.sh'
dotenv.load_dotenv(env_file, override=True)

CLIENT_ID = os.environ['CLIENT_ID']
SECRET_TOKEN = os.environ['SECRET_TOKEN']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
USER_AGENT = 'MyBot/0.0.1'

# Reddit API authentication
auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_TOKEN)
data = {'grant_type': 'password', 'username': USERNAME, 'password': PASSWORD}
header_ua = {'User-Agent': USER_AGENT}

res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=header_ua)
TOKEN = res.json()['access_token']
header_auth = {'Authorization': f"bearer {TOKEN}"}
headers = {**header_ua, **header_auth}

# Function to fetch posts from a subreddit
def fetch_posts(subreddit='femalefashionadvice'):
    res_posts = requests.get(f"https://oauth.reddit.com/r/{subreddit}/hot", headers=headers)
    posts = res_posts.json().get('data', {}).get('children', [])
    return posts

# Define the socket server
def socket_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(5)
    print("Server listening on port 9999")
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")
        
        posts = fetch_posts()
        for post in posts:
            post_data = {
                'title': post['data']['title'],
                'created_utc': post['data'].get('created_utc'),
                'text': post['data'].get('selftext', '')
            }
            print(post_data)  # Verify the data before sending
            client_socket.sendall((json.dumps(post_data) + '\n').encode('utf-8'))
        
        client_socket.close()

# Run the socket server
server_thread = threading.Thread(target=socket_server)
server_thread.start()
