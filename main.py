"""
## Overview
This script pulls random user data from randomuser.me and populates a database on my local machine.

Dependencies:
    - python 3+ (I'm running 3.8)
    - requests
    - your required database driver (I'm using postgres so psycopg2-binary)

Useful Resources:
    - Requests Documentation: https://requests.readthedocs.io/en/master/
    - Randomuser.me Documentation: https://randomuser.me/documentation
    - psycopg2 Documentation: https://www.psycopg.org/docs/
    - Real Python requests tutorial: https://realpython.com/python-requests/

Before I ran the created a local database with the name 'random_user_sample'.

## Data From randomuser.me

The data from randomuser.me comes in as the following JSON structure. Each
random user object from the "results" array is used to populate a row of the
`api_results` table. After the `api_results` table is populated, its data is
used to populate the `identities` and `profiles` tables.

```json
{
  "results": [
    {
      "gender": "male",
      "name": {
        "title": "mr",
        "first": "brad",
        "last": "gibson"
      },
      "location": {
        "street": "9278 new road",
        "city": "kilcoole",
        "state": "waterford",
        "postcode": "93027",
        "coordinates": {
          "latitude": "20.9267",
          "longitude": "-7.9310"
        },
        "timezone": {
          "offset": "-3:30",
          "description": "Newfoundland"
        }
      },
      "email": "brad.gibson@example.com",
      "login": {
        "uuid": "155e77ee-ba6d-486f-95ce-0e0c0fb4b919",
        "username": "silverswan131",
        "password": "firewall",
        "salt": "TQA1Gz7x",
        "md5": "dc523cb313b63dfe5be2140b0c05b3bc",
        "sha1": "7a4aa07d1bedcc6bcf4b7f8856643492c191540d",
        "sha256": "74364e96174afa7d17ee52dd2c9c7a4651fe1254f471a78bda0190135dcd3480"
      },
      "dob": {
        "date": "1993-07-20T09:44:18.674Z",
        "age": 26
      },
      "registered": {
        "date": "2002-05-21T10:59:49.966Z",
        "age": 17
      },
      "phone": "011-962-7516",
      "cell": "081-454-0666",
      "id": {
        "name": "PPS",
        "value": "0390511T"
      },
      "picture": {
        "large": "https://randomuser.me/api/portraits/men/75.jpg",
        "medium": "https://randomuser.me/api/portraits/med/men/75.jpg",
        "thumbnail": "https://randomuser.me/api/portraits/thumb/men/75.jpg"
      },
      "nat": "IE"
    }
  ],
  "info": {
    "seed": "fea8be3e64777240",
    "results": 1,
    "page": 1,
    "version": "1.3"
  }
}
```
"""

import requests as r
import psycopg2
from io import StringIO
import json


create_api_results_table = """
CREATE TABLE IF NOT EXISTS api_results (
    data json
)
"""

create_identities_table = """
CREATE TABLE IF NOT EXISTS identities (
    id TEXT NOT NULL PRIMARY KEY,
    username TEXT
)
"""

create_profiles_table = """
CREATE TABLE IF NOT EXISTS profiles (
    identity_id TEXT NOT NULL UNIQUE REFERENCES identities(id),
    date_of_birth TIMESTAMP,
    gender TEXT,
    state TEXT,
    city TEXT,
    zip TEXT,
    picture_url TEXT,
    cell TEXT
)
"""

populate_identities_table = """
INSERT INTO identities (id, username)
SELECT 
    CONCAT(data->'id'->'name', data->'id'->'value'),
    data->'login'->'username'
FROM api_results
ON CONFLICT (id) DO NOTHING
"""

populate_profiles_table = """
INSERT INTO profiles (identity_id, date_of_birth, gender, state, city, zip, picture_url, cell)
SELECT
    DISTINCT CONCAT(data->'id'->'name', data->'id'->'value'),
    to_timestamp(data->'dob'->>'date'::text, 'YYYY-MM-DDTHH:MI:SSS.MSZ'),
    data->>'gender',
    data->'location'->>'state',
    data->'location'->>'city',
    data->'location'->>'zip',
    data->'picture'->>'large',
    data->>'cell'
FROM api_results
ON CONFLICT (identity_id) DO NOTHING
"""


def setup_db():
    "Creates any missing tables from `api_results`, `identities`, and `profiles`"
    with psycopg2.connect(dbname="random_user_sample") as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_api_results_table)
            cursor.execute(create_identities_table)
            cursor.execute(create_profiles_table)


def get_random_users(amount=5000):
    return r.get(f"https://randomuser.me/api/?results={amount}").json()["results"]


def load_users_into_api_results(users):
    with psycopg2.connect(dbname="random_user_sample") as conn:
        with conn.cursor() as cursor:
            cursor.copy_from(StringIO(json.dumps(users)), "api_results")


def load_results_into_identities_and_profiles():
    with psycopg2.connect(dbname="random_user_sample") as conn:
        with conn.cursor() as cursor:
            cursor.execute(populate_identities_table)
            cursor.execute(populate_profiles_table)


def main():
    setup_db()
    users = get_random_users()
    load_users_into_api_results(users)
    load_results_into_identities_and_profiles()


if __name__ == "__main__":
    main()
