from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
import os
import json
import gspread
import pandas as pd
import time
import datetime
import hashlib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
print("*********************BSE Notification Batch *******************")
MaxRetry = 1
while True and MaxRetry <= 10:
    try:
        driver= webdriver.Chrome()
        url = 'https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?id=0&txtscripcd=&pagecont=&subject='
        driver.get(url)
        time.sleep(1)

        Date = (datetime.datetime.now() - datetime.timedelta(days = 7 )).strftime('%d')
        Month = (datetime.datetime.now() - datetime.timedelta(days = 7 )).strftime('%b')
        DateTo = datetime.datetime.now().strftime('%d')
        MonthTo = datetime.datetime.now().strftime('%b')
        if Date.startswith('0'):
            Date = Date[1:]
        else:
            Date
        if DateTo.startswith('0'):
            DateTo = DateTo[1:]
        else:
            DateTo
        driver.find_element(By.ID, 'ContentPlaceHolder1_txtDate').click()
        driver.find_element(By.CLASS_NAME,'ui-datepicker-month').click()
        driver.find_element(By.XPATH,f"//option[text()='{Month}']").click()
        driver.find_element(By.XPATH,f"//a[text()='{Date}']").click()
        driver.find_element(By.ID,'ContentPlaceHolder1_txtTodate').click()
        driver.find_element(By.CLASS_NAME,'ui-datepicker-month').click()
        driver.find_element(By.XPATH,f"//option[text()='{MonthTo}']").click()
        driver.find_element(By.XPATH,f"//a[text()='{DateTo}']").click()
        # dropdown = driver.find_element(By.ID, "ContentPlaceHolder1_ddlDep")
        # select = Select(dropdown)  
        # select.select_by_visible_text('Listing Operations')
        element = driver.find_element(By.ID,'ContentPlaceHolder1_btnSubmit')
        ActionChains(driver).click(element).perform()
        wait = WebDriverWait(driver, 10)
        DownloadButton = driver.find_element(By.ID,'ContentPlaceHolder1_lnkDownload')
        default_download_directory = driver.execute_script("return window.navigator.userAgent")
        default_download_directory = os.path.expanduser(r"C:\Users\Ashish Pal\Downloads")
        ActionChains(driver).click(DownloadButton).perform()
        time.sleep(5)
        driver.close()
        From_Date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime('%Y%m%d')
        To_Date = datetime.datetime.now().strftime('%Y%m%d')
        # Rename and move the downloaded file to the desired location
        downloaded_file_name = 'NotificationBSE.csv'  
        downloaded_file_path = os.path.join(r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings", downloaded_file_name)
         
        # Check if the file exists in the default download directory
        default_download_file_path = rf"C:\Users\Ashish Kumar Pal\Downloads\Notices & Circulars_{From_Date}_{To_Date}.csv" 
        if os.path.exists(default_download_file_path):
            if os.path.exists(downloaded_file_path):
                # Delete the existing file in the new download location
                os.remove(downloaded_file_path)
                print(f"Existing file '{downloaded_file_path}' deleted.")
            # Rename and move the downloaded file to the desired location
            os.rename(default_download_file_path, downloaded_file_path)
            print(f"File successfully saved as {downloaded_file_path}")
            # Load the downloaded file into a pandas DataFrame
        links_list = []
        df = pd.read_csv(downloaded_file_path,encoding='latin1',usecols=range(6))
        for i in df.loc[:,'Notice Url']:
            data=(i.split(':6443',1))
            data1= ('').join(data)
            links_list.append(data1)
        df.loc[:,'Notice Url'] = links_list
        FormateDate = [i[:8] for i in df.loc[:,'Notice No']]
        df.loc[:,'Notice No'] = FormateDate
        df.loc[:,'Notice No'] = pd.to_datetime(df['Notice No'])
        # filtered_df = df[df['Subject'].str.contains('Listing of Equity|Listing of Units|Sovereign Gold Bonds')]
        # print(filtered_df)
        # filtered_df1 = df[df['Subject'].str.contains('Change in|New ISIN|CHANGE IN')]
        # print(filtered_df1)
        gc = gspread.service_account(filename=r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Keys and Passwords\GoogleCloud(Key)\creds.json")
        spreadsheet_name = 'Notifications and Listings'
        sheet_name = 'BSE_Forthcoming_listing'
        sh = gc.open(spreadsheet_name).worksheet(sheet_name)
        print(">>> Connected to Sheet")

        BSE_FortListing = df[df['Subject'].str.contains('Listing of Equity|Listing of Units|Sovereign Gold Bonds')]
        Forth_pathNew = r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings\BSE_FortListing1.csv'
        if os.path.exists(Forth_pathNew):
            os.remove(Forth_pathNew)
        Forth_pathOld = r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings\BSE_FortListing2.csv'
        BSE_FortListing.to_csv(Forth_pathNew,index=False)
        hash_new = hashlib.sha256(open(Forth_pathNew, 'rb').read()).hexdigest()
        hash_old = None

        BSE_Changes = df[df['Subject'].str.contains('Change in|New ISIN|CHANGE IN')]
        ChangesPathNew = r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings\BSE_Changes1.csv'
        if os.path.exists(ChangesPathNew):
            os.remove(ChangesPathNew)
        ChangesPathold = r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings\BSE_Changes2.csv'
        BSE_Changes.to_csv(ChangesPathNew,index=False)
        hash_newc = hashlib.sha256(open(ChangesPathNew, 'rb').read()).hexdigest()
        hash_oldc = None

        if os.path.exists(Forth_pathOld) and os.path.exists(ChangesPathold) :
            with open(Forth_pathOld, 'rb') as f:
                hash_old = hashlib.sha256(f.read()).hexdigest()
                f.close()
            with open(ChangesPathold, 'rb') as fc:
                hash_oldc = hashlib.sha256(fc.read()).hexdigest()
                fc.close()

                if hash_new!= hash_old or hash_newc!= hash_oldc:
                    os.remove(Forth_pathOld)
                    os.rename(Forth_pathNew,Forth_pathOld)
                    os.remove(ChangesPathold)
                    os.rename(ChangesPathNew,ChangesPathold)

                    existing_data_range = sh.range('A2:L' + str(sh.row_count))
                    for cell in existing_data_range:
                        cell.value = ''

                    sh.update_cells(existing_data_range)
                    print(">>> Existing data has been Deleted ")

                    values_lists = []
                    values_lists1 = []
                    df_dict = BSE_FortListing.to_dict(orient='records')
                    df_dict1 = BSE_Changes.to_dict(orient='records')
                    for data in df_dict:
                        values_lists.append([data['Notice No'].strftime('%Y-%m-%d'),str(data['Subject']), str(data['Segment Name']), str(data['Category Name']),str(data['Department']),str(data['Notice Url'])])
                    for data1 in df_dict1:
                        values_lists1.append([data1['Notice No'].strftime('%Y-%m-%d'),str(data1['Subject']), str(data1['Segment Name']), str(data1['Category Name']),str(data1['Department']),str(data1['Notice Url'])])
                    sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
                    sh.append_row(['Forthcoming Listing'])
                    sh.append_rows(values_lists,value_input_option='USER_ENTERED')
                    sh.append_row([f'Changes in Securities'])
                    sh.append_rows(values_lists1,value_input_option='USER_ENTERED')
                    print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists)}")

                    # Trigger For Email

                    html_table1 = "<div style='text-align: center;'><h2>BSE Forthcoming Listing</h2></div>" + BSE_FortListing.to_html(index=False)
                    html_table2 = "<div style='text-align: center;'><h2>BSE Changes in Securities</h2></div>" + BSE_Changes.to_html(index=False)

                    # Step 3: Compose Email
                    with open(r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Keys and Passwords\EmailandPassword\EmailCreds.json") as config_file:
                        config = json.load(config_file)
                    sender_email = config['email_username']
                    receiver_emails = config['email recievers']
                    password = config['email_password']

                    msg = MIMEMultipart('alternative')
                    msg['From'] = sender_email
                    msg['To'] = ', '.join(receiver_emails)
                    msg['Subject'] = 'BSE Notifications and Listing'

                    # Step 4: Attach HTML Tables
                    html_content = html_table1 + html_table2
                    msg.attach(MIMEText(html_content, 'html'))

                    # Step 5: Send Email
                    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                        server.login(sender_email, password)
                        server.sendmail(sender_email, receiver_emails, msg.as_string())

                    print("Email sent successfully!")


                    # values_lists = []
                    # df_dict = BSE_FortListing.to_dict(orient='records')
                    # for data in df_dict:
                    #     values_lists.append([data['Notice No'].strftime('%Y-%m-%d'),str(data['Subject']), str(data['Segment Name']), str(data['Category Name']),str(data['Department']),str(data['Notice Url'])])
                    # sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
                    # sh.append_row(['Forthcoming Listing'])
                    # sh.append_rows(values_lists,value_input_option='USER_ENTERED')

                else:
                    os.remove(Forth_pathNew)
                    os.remove(ChangesPathNew)
                    print("No New Fothcoming Listing")
                    print("No New Changes")
        else:
            os.rename(Forth_pathNew,Forth_pathOld)
            os.rename(ChangesPathNew,ChangesPathold)

        # BSE_Changes = df[df['Subject'].str.contains('Change in|New ISIN|CHANGE IN')]
        # ChangesPathNew = r'C:\Users\Ashish Pal\Desktop\Notifications and Listings\BSE_Changes1.csv'
        # if os.path.exists(ChangesPathNew):
        #     os.remove(ChangesPathNew)
        # ChangesPathold = r'C:\Users\Ashish Pal\Desktop\Notifications and Listings\BSE_Changes2.csv'
        # BSE_Changes.to_csv(ChangesPathNew,index=False)
        # hash_newc = hashlib.sha256(open(ChangesPathNew, 'rb').read()).hexdigest()
        # hash_oldc = None
        # if os.path.exists(ChangesPathold):
        #     with open(ChangesPathold, 'rb') as f:
        #         hash_oldc = hashlib.sha256(f.read()).hexdigest()
        #         f.close()
        #         if hash_newc!= hash_oldc or var == 1:
        #             os.remove(ChangesPathold)
        #             os.rename(ChangesPathNew,ChangesPathold)

        #             values_lists1 = []
        #             df_dict1 = BSE_Changes.to_dict(orient='records')
        #             for data1 in df_dict1:
        #                 values_lists1.append([data1['Notice No'].strftime('%Y-%m-%d'),str(data1['Subject']), str(data1['Segment Name']), str(data1['Category Name']),str(data1['Department']),str(data1['Notice Url'])])
        #             sh.append_row([f'Changes in Securities'])
        #             sh.append_rows(values_lists1,value_input_option='USER_ENTERED')
        #             print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists1)}")
        #         else:
        #             os.remove(ChangesPathNew)
        #             print("Nothing New change in Securities")
        # else:
        #     os.rename(ChangesPathNew,ChangesPathold)
        # gc = gspread.service_account(filename=r'C:\Users\Ashish Pal\Desktop\PrevousLapData\Ashish\Python\Exchange Related task\BSE_Projects\creds.json')
        # spreadsheet_name = 'Notifications and Listings'
        # sheet_name = 'BSE_Forthcoming_listing'
        # sh = gc.open(spreadsheet_name).worksheet(sheet_name)
        # print(">>> Connected to Sheet")

        # existing_data_range = sh.range('A2:L' + str(sh.row_count))
        # for cell in existing_data_range:
        #     cell.value = ''

        # sh.update_cells(existing_data_range)
        # print(">>> Existing data has been Deleted ")

        # values_lists = []
        # values_lists1 = []
        # # df_dict = BSE_FortListing.to_dict(orient='records')
        # df_dict1 = filtered_df1.to_dict(orient='records')
        # # for data in df_dict:
        # #     values_lists.append([data['Notice No'].strftime('%Y-%m-%d'),str(data['Subject']), str(data['Segment Name']), str(data['Category Name']),str(data['Department']),str(data['Notice Url'])])
        # for data1 in df_dict1:
        #     values_lists1.append([data1['Notice No'].strftime('%Y-%m-%d'),str(data1['Subject']), str(data1['Segment Name']), str(data1['Category Name']),str(data1['Department']),str(data1['Notice Url'])])
        # sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
        # sh.append_row(['Forthcoming Listing'])
        # sh.append_rows(values_lists,value_input_option='USER_ENTERED')
        # sh.append_row([f'Changes in Securities'])
        # sh.append_rows(values_lists1,value_input_option='USER_ENTERED')
        # print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists)}")
        time.sleep(5)
    
        break
    except Exception as e:
        print(f"{e}")
        MaxRetry += 1