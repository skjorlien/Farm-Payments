from bs4 import BeautifulSoup
import requests
import re
import os


def download_data():
    BASEURL = "https://www.fsa.usda.gov"
    URL = "https://www.fsa.usda.gov/news-room/efoia/electronic-reading-room/frequently-requested-information/payment-files-information/index"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, 'html.parser')
    parent = soup.find_all('div', {'class': 'rxbodyfield'})

    links = parent[1].find_all(href=re.compile('NewsRoom/eFOIA'))
    paths = [link['href'] for link in links]
    paths = list(set(paths))

    for path in paths:
        fname = os.path.basename(path)
        r = requests.get(f"{BASEURL}{path}", allow_redirects=True)
        open(f'./data/raw/Public/{fname}', 'wb').write(r.content)



if __name__ == '__main__':
    # do all of them have 1 sheet? 
    import pandas as pd 

    path = './data/raw/Public'
    print(os.path.splitext(os.listdir(path)[0]))
    files = [f for f in os.listdir(path) if os.path.splitext(f)[1] == '.xlsx'] 
    
    for f in files: 
        xl = pd.ExcelFile(f"{path}/{f}")
        num = len(xl.sheet_names)
        if num == 1:
            continue 
        else: 
            raise ValueError(f'{f} had {num} sheets')
