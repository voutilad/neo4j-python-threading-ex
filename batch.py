import os
import secrets
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from neo4j import GraphDatabase

DEFAULT_URI = os.environ.get("NEO4J_URI", default="bolt://localhost:7687")
DEFAULT_USER = os.environ.get("NEO4J_USER", default="neo4j")
DEFAULT_PASS = os.environ.get("NEO4J_PASSWORD", default="password")
DEFAULT_DB = os.environ.get("NEO4J_DATABASE", default="neo4j")

_driver = None


def driver(**kwargs):
    """ Initialize (if required) and get reference to a
    neo4j driver.
    """
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            kwargs.get("uri", DEFAULT_URI),
            auth=(
                kwargs.get("user", DEFAULT_USER),
                kwargs.get("password", DEFAULT_PASS),
            ),
        )
    return _driver


def findMaxNodeId():
    with driver().session() as session:
        result = session.run("MATCH (n) RETURN max(id(n)) AS maxId")
        return result.single().get("maxId", default=-1)


def getRandomNode(maxId=2 ** 16):
    with driver().session() as session:
        rand_id = secrets.randbelow(maxId + 1)
        result = session.run(
            "MATCH (n)-[]->(x) WHERE id(n) = $randId RETURN collect(x) AS xs",
            {"randId": rand_id},
        )
        return result.single().data().get("xs", {})


def main():
    max_id = findMaxNodeId()
    workers = 4
    print(f"max node id: {max_id}")

    def work():
        return getRandomNode(max_id)

    futures = []
    completed = 0
    print("starting work...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # populate initial workers
        for _ in range(workers):
            futures.append(executor.submit(work))

        # try to blast through a lot of work
        while completed < 2 ** 10:
            (done, not_done) = wait(futures, timeout=30, return_when=FIRST_COMPLETED)
            for finished in done:
                futures.remove(finished)
                completed = completed + 1
            for _ in range(len(done)):
                futures.append(executor.submit(work))
            if completed % 128 == 0:
                print(f"finished {completed} items")

    print("done!")


if __name__ == "__main__":
    main()
