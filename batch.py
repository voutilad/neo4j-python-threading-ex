import threading
from neo4j import GraphDatabase

DEFAULT_URI="bolt://localhost:7687"
DEFAULT_USER="neo4j"
DEFAULT_PASS="password"
DEFAULT_DB="neo4j"

_driver = None

def driver(config={}):
    if _driver is None:
        _driver = GraphDatabase.driver(config.get("uri", default=DEFAULT_URI),
                                       auth=(config.get("user", default=DEFAULT_USER),
                                             config.get("password", default=DEFAULT_PASS)))
    return _driver

def main():
    driver()

if __name__ == '__main__':
    main()
