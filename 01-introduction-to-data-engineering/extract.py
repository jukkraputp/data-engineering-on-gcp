import json

import requests

def get_dog():
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()

    # Your code here
    with open(f'''dogs.json''', 'w') as fp:
        json.dump(data, fp)


if __name__ == "__main__":
    get_dog()