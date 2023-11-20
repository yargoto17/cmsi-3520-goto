import mechanicalsoup as ms
import redis



def crawl(browser, r, url):
    #Download url
    browser.open(url)

    # Parse for more urls
    a_tags = browser.page.find_all("a")
    hrefs = [ a.get("href") for a in a_tags ]
    #Do wikipedia url specific filtering
    wikipedia_domain = "https://en.wikipedia.org"
    links = [ wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    #print(hrefs)

    # Put urls in Redis Queue

    r = redis.Redis()
    # create a linked list in Redis, call it "links"
    print("Pushing links onto Redis")
    r.lpush("links", *links)

# while (r.llen("links") > 0):
#     crawl(r.pop("link"))

browser = ms.StatefulBrowser()
r = redis.Redis()

start_url = "https://en.wikipedia.org/wiki/Redis"

r.lpush("links", start_url)
while link := r.rpop("links"):
    if "Jesus" in str(link):
        break
    crawl(browser, r, link)

