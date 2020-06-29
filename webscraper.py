import requests
from urllib.error import HTTPError, URLError 
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
import time
import pickle
import sys
sys.setrecursionlimit(1000000)

# LOGGER:

# configure logger to keep track of progress:
# create logger
logger = logging.getLogger(name='webscraper')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '[%(asctime)s %(lineno)s %(levelname)s] - %(message)s'
    ) 
# add console Stream Handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler) # add it to logger
# add File Handler
file_handler = logging.FileHandler('webscraper.log', 'w')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) # add it to logger



# REQUEST
def getPage(url):
    ''' 
        Requests a webpage and returns a response object.
        If any exceptions are presented, it logs the error and
        returns None. 

        If the request times out it will retry 3 times.

        Requires import requests and time modules.

        url = url to be parsed e.g.:'http://www.example.com/'
    '''
   
    headers = {'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/513.42 (KHTML, like Gecko) Ubuntu Chromium/34.0.1435.110 Chrome/34.0.1435.110 Safari/513.42'}
    try:
        time.sleep(3) # Timing requests to avoid overloading server
        response = requests.get(url.strip(), headers=headers, timeout=10)
    except TimeoutError:
        attemps = 3 # Retry 3 times
        logger.error('TimeoutError, retrying 3 times')
        while attemps > 0:
            try:
                time.sleep(3) # Timing requests to avoid overloading server
                response = requests.get(url, headers=headers, timeout=10)
                break
            except TimeoutError:
                attemps -= 1
                return None
            except:
                return None
    except:
        return None
    else:
        return response.content
                
# BEAUTIFULSOUP
def parsePage(content):
    ''' 
        Parses an HTML object using BeautifulSoup with 'html.parser'.
        If any exceptions are presented, it logs the error and
        returns None. 

        Requires import requests time modules.
    '''
    try:
        bs = BeautifulSoup(content, 'html.parser') # Instantiating Beautiful Soup
        logger.info("HTML for page '{}' parsed with BeautifulSoup".format(bs.title.text))
        return bs
    except TypeError as e:
        logger.error('Page not parsed by BeautifulSoup')
        return None


# MAIN

def main():
    start_url = "https://en.wikipedia.org/wiki/Category:Lists_of_companies_by_country" # Start at this page    
    content = getPage(start_url) # Make request to url
    bs = parsePage(content) # Load to Beautiful Soup

    # Parse subcategories containing each country "List of companies from {}" 
    category_group = bs.find_all('div', {'class':'mw-category'})[1]
    logger.info('Created category group')
    with open('category_group.pickle', 'wb') as f:
        pickle.dump(category_group, f)

    # Convert it to a list of tubles [(category, URI)]
    categories_list = [ (a.text, f"https://en.wikipedia.org/{a['href']}") for a in category_group.find_all('a')]  # create list of tuples (category, URI)
    logger.info(f"Created list of categories contained in page {bs.title.text}")
    with open('category_list.pickle', 'wb') as f:
        pickle.dump(categories_list, f)
    

    # Create master dataframe that will store all data:
    columns = ['Company Name', 'Country', 'Type', 'Industry', 'Website','URL', 
                'Number of employees', 'Revenue', 'Net income', 'ISIN', 
                'Headquarters', 'Headquarters location'
              ]
    master_df = pd.DataFrame(columns=columns)
    # Loop through each country and get link and country name:
    for country in range(len(categories_list)):
        title = categories_list[country][0]
        link = categories_list[country][1]
        country = categories_list[country][0].split()[4:][0] # Get country name out of the list of categories
        
        #Get Notable Companies list if exist, else continue to next country:
        try:
            cont = getPage(link)
            soup = parsePage(cont)
            table = soup.find_all(text=re.compile("Notable companies\s"))[0].parent.parent
            notable_companies_links = []
            for tr in table.find_all('tr')[1:]:
                notable_companies_links.append((tr.a['title'], f"https://en.wikipedia.org/{tr.a['href']}"))
            logger.info(f"Created list of companies, link for {country}")
        
            # Go to each company page and convert Infobox table into a Pandas Dataframe
            for item in range(len(notable_companies_links)):
                html = getPage(notable_companies_links[item][1]) # make request
                company_name = notable_companies_links[item][0]
                try:
                    dfx = pd.read_html(html, attrs={'class':'infobox'})[0].dropna().transpose() # convert infobox to DataFrame
                    dfx.columns = dfx.iloc[0] # Copy first row and make it columns
                    dfx = dfx.drop(0, axis=0) 
                    dfx['Country'] = country
                    dfx['Company Name'] = company_name # add company name
                    # Rearange columns to have Company name first
                    cols = dfx.columns.to_list() 
                    cols = cols[-1:] + cols[:-1]
                    dfx = dfx[cols]
                    # Append data to master 
                    master_df = pd.concat([master_df, dfx], axis=0, ignore_index=True) # Add company info to Master Dataframe
                    logger.info(f"Appended data for {company_name}")
                except:
                    logger.error(f"Could not create Dataframe for {company_name} from Notable Companies list")
                    continue
        except:
            logger.error(f"Could not parse Notable Companies Table for {country}")
            continue    
        
        logger.info(f"Complete for {country}")
        master_df = master_df[columns]
        with open('master_df.pickle', 'wb') as f:
            pickle.dump(master_df, f)       
        logger.info(f"A total of {master_df.shape[0]} rows and {master_df.shape[1]} columns so far loaded to master!")       

        

if __name__ == '__main__':
    main() 
