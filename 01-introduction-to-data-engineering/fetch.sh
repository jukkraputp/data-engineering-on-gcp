#!/bin/bash

API_KEY='$2a$10$ln7yf9d3I.Vazu7VoPdsbO8mu64dPRHlfGV65VBMZ0ficoaPYWFwG'
COLLECTION_ID='659a4478266cfc3fde739825'

curl -XGET \
    -H "X-Master-key: $API_KEY" \
    "https://api.jsonbin.io/v3/c/$COLLECTION_ID/bins"