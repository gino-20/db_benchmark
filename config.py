from dotenv import load_dotenv
import os

load_dotenv()

data_range = 100

pg_config = {
    'dbname': os.environ.get("PG_DBNAME"),
    'user': os.environ.get("PG_DBUSER"),
    'password': os.environ.get("PG_DBPASS"),
    'host': os.environ.get("PG_DBHOST"),
    'port': os.environ.get("PG_DBPORT"),
}

elk_url = os.environ.get("ELK_URL")
elk_index = {
              "settings": {
                "refresh_interval": "1s",
                "analysis": {
                  "filter": {
                    "english_stop": {
                      "type":       "stop",
                      "stopwords":  "_english_"
                    },
                    "english_stemmer": {
                      "type": "stemmer",
                      "language": "english"
                    },
                    "english_possessive_stemmer": {
                      "type": "stemmer",
                      "language": "possessive_english"
                    },
                    "russian_stop": {
                      "type":       "stop",
                      "stopwords":  "_russian_"
                    },
                    "russian_stemmer": {
                      "type": "stemmer",
                      "language": "russian"
                    }
                  },
                  "analyzer": {
                    "ru_en": {
                      "tokenizer": "standard",
                      "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer"
                      ]
                    }
                  }
                }
              },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "user_id": {
                "type": "keyword"
            },
            "likes": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "dislikes": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "bookmarks": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "score": {
                "type": "float",
            }
        }
    }
}

mongo_url = os.environ.get('MONGO_URL')

clickhouse_dsl = {
    'host': os.environ.get('CLICKHOUSE_URL'),
    'username': os.environ.get('CLICKHOUSE_USER'),
    'password': os.environ.get('CLICKHOUSE_PASS')
}
