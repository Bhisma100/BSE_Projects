import gspread
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime
import time

print('************** BSE Listing Batch *******************')
while True:
    try:
        s = requests.Session()
        headerbse ={
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

        DateFrom = (datetime.datetime.now() - datetime.timedelta(days = 7)).strftime('%Y%m%d')
        DateTo = datetime.datetime.now().strftime('%Y%m%d')

        url = f'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=1&strCat=New+Listing&strPrevDate={DateFrom}&strScrip=&strSearch=P&strToDate={DateTo}&strType=C&subcategory=-1'
        page = s.get(url,headers= headerbse)
        page.json()
        if 'Table' in page.json():
            Data = page.json()['Table']
            print(">>> Data Fetched.")

            # Connecting to GoogleSheet
            gc = gspread.service_account(filename=r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Exchange Related task\NSE_Projtects\creds.json')
            spreadsheet_name = 'Notifications and Listings'
            sheet_name = 'BSE_Listing'
            sh = gc.open(spreadsheet_name).worksheet(sheet_name)
            print(">>> Connected to Sheet")

            existing_data_range = sh.range('A2:L' + str(sh.row_count))
            for cell in existing_data_range:
                cell.value = ''

            sh.update_cells(existing_data_range)
            print(">>> Existing data has been Deleted ")

            df = pd.DataFrame(Data)
            print(df)
            values_lists = []
            df_dict = df.to_dict(orient='records')
            print(f">>> Number Rows to be Updated are: {len(df_dict)}")
            for data in df_dict:
                values_lists.append([str(data['NEWS_DT']),str(data['SCRIP_CD']), str(data['SLONGNAME']), str(data['NSURL']),str(data['SUBCATNAME'])])
            sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
            sh.append_rows(values_lists)
            print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists)}")
            time.sleep(5)

            break
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Retrying the script...")
