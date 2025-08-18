user_schema = {
    "id": "INTEGER PRIMARY KEY",
    "name": "TEXT NOT NULL",
    "email": "TEXT NOT NULL"
}

activity_schema = {
    "user_id": "INTEGER",
    "action": "TEXT NOT NULL",
    "timestamp": "TEXT NOT NULL"
}