INDEX = {
    'SCHEMA': 'dbo',
    'TABLE': 'wikipedia_articles_embeddings',
    'COLUMN': {
        'ID': 'id',
        'VECTOR': 'content_vector'
    },
    'VECTOR': {
        'DIMENSIONS': 1536,
    }
}

BACKGROUND_JOBS = {
    "CHANGE_TRACKING_CRONTAB": "*/1 * * * * *",
    "SAVE_INDEX_CRONTAB": "0 */1 * * * *"
}