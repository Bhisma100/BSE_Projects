from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import datetime
import gspread
import pandas as pd
import time

# Connecting using selenium to connect tp the website
header = {
'scheme':'https',
'Accept':'application/json, text/plain, */*',
'Accept-Language':'en-US,en;q=0.8',
'Referer':'https://www.bseindia.com/',
'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"',
'Sec-Ch-Ua-Mobile': '?0',
'Sec-Ch-Ua-Platform':'"Windows"',
'Sec-Fetch-Mode':'cors',
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

chrome_options = Options()
chrome_options.add_argument('--headless')
for key, value in header.items():
    chrome_options.add_argument(f"{key}={value}")
print("Running Chrome in headless mode")
driver = webdriver.Chrome(options=chrome_options)
url = 'https://www.bseindia.com/corporates/Forth_Results.html'
driver.get(url)
source = driver.page_source

if 'table' in  source:
    soup = bs(source,'html.parser')
    Table = soup.find('table',{'class':'ng-scope'})
    table_content = []
    values_lists = []
    rows = Table.find_all('tr')
    for row in rows:
        cells = row.find_all(['td', 'th'])
        row_content = [cell.get_text(strip=True) for cell in cells]
        table_content.append(row_content)
    print(">>> Table has been extracted from site")
    Column = table_content[1]
    MainDf = table_content[2:]
    df = pd.DataFrame(MainDf,columns = Column)

    # Connecting to the Sheet
    gc = gspread.service_account(filename=r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Keys and Passwords\GoogleCloud(Key)\creds.json")
    spreadsheet_name = 'QuarterlyResultsTracker'
    sheet_name = 'Result calendar'
    sh = gc.open(spreadsheet_name).worksheet(sheet_name)
    print(">>> Connedcted to GoogleSheet")

    # Deleting existing data from GoogleWorksheet
    existing_data_range = sh.range('A2:F' + str(sh.row_count))
    for cell in existing_data_range:
        cell.value = ''

    sh.update_cells(existing_data_range)
    print(">>> Exsiting Data has been Deleted from the sheet")


    df['Result Date'] = pd.to_datetime(df['Result Date'])
    Data_Dict = df.to_dict(orient='records')
    for data in Data_Dict:
        values_lists.append([int(data['Security Code']), str(data['Security Name']),data['Result Date'].strftime('%Y-%m-%d')])
    sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
    sh.append_rows(values_lists,value_input_option='USER_ENTERED')
    print(">>> New Data has been Added to the sheet")
    time.sleep(2)

else:
    print(f"Response from server: {driver.page_source}")