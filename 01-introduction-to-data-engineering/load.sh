#!/bin/bash

API_KEY='$2a$10$ln7yf9d3I.Vazu7VoPdsbO8mu64dPRHlfGV65VBMZ0ficoaPYWFwG'
COLLECTION_ID='659a4478266cfc3fde739825'

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @dogs.json \
    "https://api.jsonbin.io/v3/b"