import requests
import sys
import json
import logging
import time
import json
import socket

from confluent_kafka import Producer, Consumer, KafkaException
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

DEFAULT_PATH = "/eventservice/controller/v1"  # Change this path as needed

SUBJECT_ID = None
CLIENT_ID = None
CLIENT_SECRET = None
SCOPE = None
TOKEN_URL = None
REALM_ID = None
EFS_HOST = None
REQUEST_HEADER = None
TOPIC_NAME = None
GROUP_ID = None
EXT_BOOTSTRAP_SERVER = None
INT_BOOTSTRAP_SERVER = None
STOP_CONSUMER_AFTER_SECONDS = 10
BOOTSTRAP_SERVER_CONNECTION_RETRY_COUNT = 5
DELAY_IN_CONNECTION_RETRY = 1.0
NUMBER_OF_MESSAGE_TO_PRODUCE = 2
ENV_NAME = None
# logger = logging.getLogger("custom")

def set_globals_from_json(json_str):
    global CLIENT_ID, CLIENT_SECRET, SUBJECT_ID, REALM_ID, SCOPE, TOKEN_URL, EFS_HOST, EXT_BOOTSTRAP_SERVER, INT_BOOTSTRAP_SERVER

    try:
        config_json = json.loads(json_str)  # Convert string to dict
    except json.JSONDecodeError as e:
        print("Invalid JSON input:", e)
        sys.exit(1)

    SUBJECT_ID = config_json.get("SUBJECT_ID")
    CLIENT_ID = config_json.get("CLINET_ID")
    CLIENT_SECRET = config_json.get("CLIENT_SECRET")
    SCOPE = config_json.get("SCOPE")
    TOKEN_URL = config_json.get("TOKEN_URL")
    REALM_ID = config_json.get("REALM_ID")
    EFS_HOST = config_json.get("EFS_HOST")
    EXT_BOOTSTRAP_SERVER = config_json.get("EXT_BOOTSTRAP_SERVER")
    INT_BOOTSTRAP_SERVER = config_json.get("INT_BOOTSTRAP_SERVER")

    get_access_token()

def get_access_token():
    global REQUEST_HEADER
    data = {
        "grant_type": "client_credentials",
        "CLIENT_ID": CLIENT_ID,
        "CLIENT_SECRET": CLIENT_SECRET,
        "scope": SCOPE
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(TOKEN_URL, data=data, headers=headers)
    print(f"Token response status: {response.status_code}")
    # print(f"Token response body: {response.text}")
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        print(f"Access token received")

        REQUEST_HEADER = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    else:
        print(f"Failed to obtain token: {response.status_code} {response.text}")

def create_resources_if_not_found():
    global EFS_HOST, token, SUBJECT_ID, REALM_ID, REQUEST_HEADER
    if EFS_HOST.endswith("/"):
        EFS_HOST = EFS_HOST[:-1]
    
    full_url = EFS_HOST + DEFAULT_PATH

    try:

        with open(f"{ENV_NAME}/project.json", "r") as f:
            payload = json.load(f)

        project_name = payload.get('name')
        print(f"Project name: {project_name}")

        get_response = requests.get(full_url +"/projects/"+ project_name, headers=REQUEST_HEADER)

        print(f"Get project response status: {get_response.status_code}")
        # print(f"Get project response body: {get_response.text}")

        if get_response.status_code == 404:
            print("GET returned 404. Making POST request...")
            post_response = requests.post(full_url + "/projects", headers=REQUEST_HEADER, json=payload)
            print(f"Project create response status: {post_response.status_code}")
            # print(f"Project create response body: {post_response.text}")

        else:
            print(f"Project found")
        
        createNameSpaceIfNotFound(project_name)

    except requests.RequestException as e:
        print(f"Project check failed: {e}")


def createNameSpaceIfNotFound(project_name):
    global EFS_HOST, token, SUBJECT_ID, REALM_ID, REQUEST_HEADER
    try:
        full_url = EFS_HOST + DEFAULT_PATH  

        with open(f"{ENV_NAME}/namespace.json", "r") as f:
            payload = json.load(f)
        
        namespace_name = payload.get('name')
        print(f"Namespace name: {namespace_name}")

        
        get_response = requests.get(full_url +"/projects/"+project_name+ "/namespaces/"+namespace_name, headers=REQUEST_HEADER)
  
        print(f"Get namespace response status: {get_response.status_code}")
        # print(f"Get namespace response body: {get_response.text}")

        if get_response.status_code == 404:
            print("Namespace not found, creating...")
            post_response = requests.post(full_url +"/projects/"+project_name+ "/namespaces", headers=REQUEST_HEADER, json=payload)
            print(f"Namespace create response status: {post_response.status_code}")
            # print(f"Namespace create response body: {post_response.text}")
        else:
            print(f"Namespace found")

        createTopicIfNotFound(project_name, namespace_name)

    except requests.RequestException as e:
        print(f"Namespace check failed: {e}")

def createTopicIfNotFound(project_name, namespace_name):
    global TOPIC_NAME, GROUP_ID, REQUEST_HEADER
    try:
        full_url = EFS_HOST + DEFAULT_PATH  

        with open(f"{ENV_NAME}/topic.json", "r") as f:
            payload = json.load(f)

        topic_name = getTopicPrefix(payload[0].get('type'), payload[0].get('entityOrEventName'))
        print(f"Topic name: {topic_name}")

        get_response = requests.get(full_url +"/topics/"+topic_name, headers=REQUEST_HEADER)

        print(f"Get topic response status: {get_response.status_code}")
        # print(f"Get topic response body: {get_response.text}")

        if get_response.status_code == 404:
            print("Topic not found, creating...")
            post_response = requests.post(full_url +"/projects/"+project_name+ "/namespaces/"+namespace_name+"/topics", headers=REQUEST_HEADER, json=payload)
            print(f"Topic create response status: {post_response.status_code}")
            # print(f"Topic create response body: {post_response.text}")

        else:
            print(f"Topic found")

        TOPIC_NAME = topic_name
        GROUP_ID = project_name
        createOauthPolicyIfNotFound(project_name)


    except requests.RequestException as e:
        print(f"Topic check failed: {e}")


def createOauthPolicyIfNotFound(project_name):
    global EFS_HOST, SUBJECT_ID, REALM_ID, REQUEST_HEADER
    try:
        full_url = EFS_HOST + DEFAULT_PATH  

        policy_name = "sample-policy"

        payload = {
            "oauth2PolicyName": policy_name,
            "oauth2Claim": {
                "sub": SUBJECT_ID,
                "by_realm_id": REALM_ID
            }
        }

        print(f"Policy name: {policy_name}")

        get_response = requests.get(full_url +"/projects/"+project_name+"/oauth2-policy/"+policy_name, headers=REQUEST_HEADER)

        print(f"Get Oauth policy response status: {get_response.status_code}")
        # print(f"Get Oauth policy response body: {get_response.text}")

        if get_response.status_code in [204, 400, 404]:
            print("Oauth policy not found, creating...")
            post_response = requests.post(full_url +"/projects/"+project_name+ "/oauth2-policy", headers=REQUEST_HEADER, json=payload)
            print(f"Oauth policy create response status: {post_response.status_code}")
            # print(f"Oauth policy create response body: {post_response.text}")
        else:
            print(f"Oauth policy found")

    except requests.RequestException as e:
        print(f"Oauth policy check failed: {e}")



def getTopicPrefix(topic_type, entity_name):
    global EFS_HOST, token, SUBJECT_ID, REALM_ID, REQUEST_HEADER
    try:
        with open(f"{ENV_NAME}/namespace.json", "r") as f:
            namespace = json.load(f)

        topic_prefix = namespace.get('topicNamePrefix', {})
        environment = topic_prefix.get('environment', '')
        region = topic_prefix.get('region', '')
        customer = topic_prefix.get('customer', '')
        domain = topic_prefix.get('domain', '')

        prefix_string = f"{environment}.{region}.{customer}.{domain}.{entity_name}.{topic_type}"
        return prefix_string

    except Exception as e:
        print(f"Failed to load or parse namespace.json: {e}")
        return ""


def get_token(config):

    token_url = (
    f"{TOKEN_URL}"
    f"&realmName={REALM_ID}"
    f"&grant_type=client_credentials"
    f"&client_id={CLIENT_ID}"
    f"&client_secret={CLIENT_SECRET}"
    f"&scope={SCOPE}"
    )

    # print(f"token_url: {token_url}")

    client = BackendApplicationClient(client_id=f"{CLIENT_ID}")
    oauth = OAuth2Session(client=client)
    token_json = oauth.fetch_token(token_url=token_url,client_id=f"{CLIENT_ID}",client_secret=f"{CLIENT_SECRET}")
    token = token_json["access_token"]
    expires_in = time.time() + float(token_json["expires_in"])

    # print(f"Token: {token}")
    return token, expires_in

def produce():

    configs = [
        {
            "bootstrap.servers": f"{EXT_BOOTSTRAP_SERVER}"
        },
        {
            "bootstrap.servers": f"{INT_BOOTSTRAP_SERVER}"
        }
    ]
    
    for config in configs:
        producer_props = {
            **config,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": get_token,
            # "logger": logger
        }

        bootstarp_server = config.get('bootstrap.servers')

        if(is_bootstarp_server_available(bootstarp_server)):
            print(f"Producing to: {bootstarp_server}")
            try:
                producer = Producer(producer_props)
                for i in range(NUMBER_OF_MESSAGE_TO_PRODUCE):
                    data = f"Hello world!!! #{i+1}".encode("utf-8")
                    producer.produce(topic=f"{TOPIC_NAME}", value=data, on_delivery=acked)
                    producer.flush()
                    time.sleep(1)
            except Exception as e:
                print(f"Error while producing: {e}")
            finally:
                print(f"Producer closed after producing {NUMBER_OF_MESSAGE_TO_PRODUCE} messages.")

def consume():

    configs = [
        {
            "bootstrap.servers": f"{EXT_BOOTSTRAP_SERVER}",
            "group.id": f"{GROUP_ID}.external"
        },
        {
            "bootstrap.servers": f"{INT_BOOTSTRAP_SERVER}",
            "group.id": f"{GROUP_ID}.internal"
        }
    ]


    for config in configs:
        consumer_props = {
            **config,
            "auto.offset.reset": "earliest",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": get_token,
            # "logger": logger
        }
        
        bootstarp_server = config.get('bootstrap.servers')

        if(is_bootstarp_server_available(bootstarp_server)):
            print(f"Consuming from: {bootstarp_server} with group id: {config.get('group.id')}")
            consumer = Consumer(consumer_props)
            consumer.subscribe([f"{TOPIC_NAME}"])
            
            last_message_time = time.time()
            
            try:
                while True:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        if time.time() - last_message_time > STOP_CONSUMER_AFTER_SECONDS:
                            print(f"No new messages for {STOP_CONSUMER_AFTER_SECONDS} seconds. Exiting.")
                            break
                        continue
                    if msg.error():
                        raise KafkaException(msg.error())
                    else:
                        print(f"Message consumed: {msg.value().decode('utf-8')} from {msg.topic()} [{msg.partition()}] offset {msg.offset()}")
                        last_message_time = time.time()  # reset timeout
            except Exception as e:
                print(f"Error while consuming: {e}")
            finally:
                consumer.close()
                print("Consumer closed.")

def is_bootstarp_server_available(bootstrap_server: str) -> bool:
    host, port = bootstrap_server.split(":")

    for attempt in range(1, BOOTSTRAP_SERVER_CONNECTION_RETRY_COUNT + 1):
        try:
            with socket.create_connection((host, int(port)), timeout=2.0):
                print(f"Connected to Kafka at {bootstrap_server} on attempt {attempt}")
                return True
        except Exception as e:
            print(f"[Attempt {attempt}] Kafka not reachable at {bootstrap_server} → {e}")
            if attempt < BOOTSTRAP_SERVER_CONNECTION_RETRY_COUNT:
                time.sleep(DELAY_IN_CONNECTION_RETRY)
    return False


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg.key() if msg.key() else 'No Key'}: {err}")
    else:
        print(f"Message: {msg.value().decode('utf-8')} produced to topic successfully: {msg.topic()} in partition [{msg.partition()}] at offset {msg.offset()}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run-kafka-test.py <TOKEN_SECRETS_JSON>")
        sys.exit(1)

    JSON_CREDS = sys.argv[1]
    ENV_NAME = sys.argv[2]
    set_globals_from_json(JSON_CREDS)
    create_resources_if_not_found()
    print(f"Starting producer....")
    produce()
    print(f"Starting consumer....")
    consume()