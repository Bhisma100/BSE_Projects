from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
import gspread
import pandas as pd
import time
import datetime
print("*********************BSE Notification Batch *******************")
while True:
    try:
        header ={
        'scheme':'https',
        'Accept':'application/json, text/plain, */*',
        'Accept-Language':'en-US,en;q=0.8',
        'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform':'"Windows"',
        'Sec-Fetch-Mode':'cors',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        for key, value in header.items():
            chrome_options.add_argument(f"--{key}={value}")
        driver = webdriver.Chrome(options=chrome_options)
        #driver.maximize_window()
        url = 'https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?id=0&txtscripcd=&pagecont=&subject='
        driver.get(url)
        time.sleep(1)

        Date = (datetime.datetime.now() - datetime.timedelta(days = 7 )).strftime('%d')
        Month = (datetime.datetime.now() - datetime.timedelta(days = 7 )).strftime('%b')
        if Date.startswith('0'):
            Date = Date[1:]
        else:
            Date
        driver.find_element(By.ID, 'ContentPlaceHolder1_txtDate').click()
        driver.find_element(By.CLASS_NAME,'ui-datepicker-month').click()
        driver.find_element(By.XPATH,f"//option[text()='{Month}']").click()
        driver.find_element(By.XPATH,f"//a[text()='{Date}']").click()
        dropdown = driver.find_element(By.ID, "ContentPlaceHolder1_ddlDep")
        select = Select(dropdown)  
        select.select_by_visible_text('Listing Operations')
        element = driver.find_element(By.ID,'ContentPlaceHolder1_btnSubmit')
        ActionChains(driver).click(element).perform()
        wait = WebDriverWait(driver, 10)
        time.sleep(1)
        PageNo = []
        DataList = []
        Links_List = [] 
        BSE_Circular = []
        r = driver.page_source
        soup = bs(r,'html.parser')
        Table = soup.find('table',{'id':'ContentPlaceHolder1_GridView2'})
        Rows = Table.find_all('tr')
        for row in Rows[-1:]:
            Datas = row.find_all('td')
            for data in Datas:
                PageNo.append(data.text.strip())
        for row in Rows[1:21]:
            Datas = row.find_all('td')
            link = f'https://www.bseindia.com{row.find('a').get('href')}'
            Links_List.append(link)
            for data in Datas[:5]:
                DataList.append(data.text.strip('\n'))
        print('>>> page 1 extracted')
                    
        for i in range(2,len(PageNo)):
            element = wait.until(EC.element_to_be_clickable((By.XPATH, f"//a[text()='{i}']")))
            driver.execute_script("arguments[0].scrollIntoView();", element)
            driver.execute_script("arguments[0].click();", element)
            time.sleep(5)
            r = driver.page_source
            soup = bs(r,'html.parser')
            Table = soup.find('table',{'id':'ContentPlaceHolder1_GridView2'})
            Rows = Table.find_all('tr')
            for row in Rows[1:21]:
                Datas = row.find_all('td')
                link = f'https://www.bseindia.com{row.find('a').get('href')}'
                Links_List.append(link)
                for data in Datas[:5]:
                    DataList.append(data.text.strip('\n'))
            print(f'>>> page no {i} extracted')
        for i in range(0,len(DataList),5):
            PDFormate = DataList[i:i+5]
            BSE_Circular.append(PDFormate)
        df = pd.DataFrame(BSE_Circular,columns=['Date','Subject','Segment Name','Category Name','Department'])
        df['Links'] = Links_List

        Filtered_df = df[df['Subject'].str.contains('Listing of Equity')]
        gc = gspread.service_account(filename=r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Exchange Related task\NSE_Projtects\creds.json')
        spreadsheet_name = 'Notifications and Listings'
        sheet_name = 'BSE_Forthcoming_listing'
        sh = gc.open(spreadsheet_name).worksheet(sheet_name)
        print(">>> Connected to Sheet")

        existing_data_range = sh.range('A2:L' + str(sh.row_count))
        for cell in existing_data_range:
            cell.value = ''

        sh.update_cells(existing_data_range)
        print(">>> Existing data has been Deleted ")


        values_lists = []
        df_dict = Filtered_df.to_dict(orient='records')
        print(f">>> Number Rows to be Updated are: {len(df_dict)}")
        for data in df_dict:
            values_lists.append([str(data['Date']),str(data['Subject']), str(data['Segment Name']), str(data['Category Name']),str(data['Department']),str(data['Links'])])
        sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
        sh.append_rows(values_lists)
        print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists)}")
        time.sleep(5)
        break
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Retrying the script...")
                    
