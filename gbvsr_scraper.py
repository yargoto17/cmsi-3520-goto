import mechanicalsoup as ms
import sqlite3

start_url = "https://www.dustloop.com/w/GBVSR"
heading = "https://www.dustloop.com/"
browser = ms.StatefulBrowser()
characters_links = []
names = []
list_of_pros = []
list_of_cons = []

browser.open(start_url)

ul_elements = browser.page.find_all("li", attrs={"class": "gallerybox"})
if ul_elements:
    for ul_element in ul_elements:
        a_elements = ul_element.select("a")

        for a_element in a_elements:
            # Print or process the href attribute of each <a> tag
            href_value = a_element.get('href')
            name_parts = href_value.split("w/GBVSR/")
            if len(name_parts) >= 1:
                names.append(name_parts[1])
                characters_links.append(heading + href_value)



def crawl(browser, url, position):
    #Download url
    browser.open(url)
    print(url)
    for spantip in browser.page.find_all('span', attrs={'class': 'tooltiptext'}):
        spantip.extract()
    reason_to_pick_element = browser.page.find_all("td", attrs={"class": "ReasonsToPick-entry", "id": "picklist"})
    if reason_to_pick_element:
        list_elements = reason_to_pick_element[0]('ul')

        if list_elements:
            li_elements = list_elements[0].select('li')
            reasons_yes = []
            for li_element in li_elements:
                reasons_yes.append(li_element.text)
            list_of_pros.append(reasons_yes)
    else:
        reason_to_pick_element = browser.page.find_all("div", attrs={"class": "Pros"})
        if reason_to_pick_element:
            list_elements = reason_to_pick_element[0]('ul')

            if list_elements:
                li_elements = list_elements[0].select('li')
                reasons_yes = []
                for li_element in li_elements:
                    reasons_yes.append(li_element.text)
                list_of_pros.append(reasons_yes)
    
    if reason_to_pick_element:
        pass
    else:
        list_of_pros.append(["Unknown"])
                
    reason_to_not_pick = browser.page.find_all("td", attrs={"class": "ReasonsToPick-entry", "id": "avoidlist"})
    if reason_to_not_pick:
        list_elements = reason_to_not_pick[0]('ul')

        if list_elements:
            li_elements = list_elements[0].select('li')
            reasons_no = []
            for li_element in li_elements:  
                reasons_no.append(li_element.text)
            list_of_cons.append(reasons_no)
    else:
        reason_to_not_pick = browser.page.find_all("div", attrs={"class": "Cons"})
        if reason_to_not_pick:
            list_elements = reason_to_not_pick[0]('ul')

            if list_elements:
                li_elements = list_elements[0].select('li')
                reasons_no = []
                for li_element in li_elements:  
                    reasons_no.append(li_element.text)
                list_of_cons.append(reasons_no)
    
    if reason_to_not_pick:
        pass
    else:
        list_of_cons.append(["Unknown"])

counter_names = 0 
for character in characters_links :
    
    crawl(browser, character, counter_names)
print(names)
print(list_of_pros)
print(list_of_cons)


# create new database and cursor
connection = sqlite3.connect("GBVSR.db")
cursor = connection.cursor()
# create database table and insert all data frame rows
cursor.execute('''
    CREATE TABLE IF NOT EXISTS gbvsr(
        id INTEGER PRIMARY KEY,
        name TEXT,
        pros TEXT,
        cons TEXT
    )
''')
reasons_to_pick_str = str(list_of_pros)
reasons_to_not_pick_str = str(list_of_cons)

counter = 0
max_length = names.__len__()
while (counter < (max_length)):
    cursor.execute('''
        INSERT INTO gbvsr (name, pros, cons)
        VALUES (?, ?, ?)
    ''', (names[counter], str(list_of_pros[counter]), str(list_of_cons[counter])))
    connection.commit()
    counter += 1

connection.close()