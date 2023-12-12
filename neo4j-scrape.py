import mechanicalsoup as ms
import configparser
import redis
from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase

class Neo4JConnector:
    def __init__(self, uri, user, password) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user,password))
    def close(self):
        self.driver.close()
    def add_links(self,page,links):
        with self.driver.session() as session:
            session.execute_write(self._create_links, page, links)
    def flush_db(self):
        print("clearing graph db")
        with self.driver.session() as session:
            session.execute_write(self._flush_db)
    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        tx.run("CREATE (:Page { url: $page })", page=page)
        for link in links:
            tx.run("MATCH (p:Page) WHERE p.url = $page "
                   "CREATE (:Page {url: $link}) -[:LINKS_TO] -> (p)",
                   page=page, link=link)
    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")
        
neo = Neo4JConnector("bolt://35.171.21.80:7687","neo4j","spears-rocks-irons")
neo.flush_db()
config = configparser.ConfigParser()
config.read('example.ini')
# es = None
es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    basic_auth=(config['ELASTIC']['user'],config['ELASTIC']['password']))
print(es.info())

def write_to_elastic(es,url,html):
    # link = url.decode('utf-8')
    es.index(
    index='webpages',
    document={
        'url' : url.decode('utf-8'),
        'html' : html
    }
    )   

# result = es.search(
#     indices='webpage',
#     query={
#         'match': {'html' : 'html'}
#     }
# )

def crawl(browser, r, es, neo, url):
    # Download url
    print("Downloading url")
    browser.open(url)
    # Cache page to elasticsearch
    write_to_elastic(es,url,str(link))

# Parse for more urls
    print("Parsings for more link")
    a_tags = browser.page.find_all("a")
    hrefs = [ a.get("href") for a in a_tags]
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    r = redis.Redis()
    print("Pushing links into Redis")
    r.lpush("links", *links)
    neo.add_links(url,links)

# Put urls in Redis queue
browser = ms.StatefulBrowser()
r = redis.Redis()
r.flushall()

# while r.llen("links") > 0:
#     crawl(r.rpop("links"))
start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)
while link := r.rpop("links"):
    if "Jesus" in str(link):
        break
    crawl(browser,r,es, neo, link)

neo.close()