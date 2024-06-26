import gspread
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime
import time
import os
import hashlib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json

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

            #Connecting to GoogleSheet
            gc = gspread.service_account(filename=r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Keys and Passwords\GoogleCloud(Key)\creds.json")
            spreadsheet_name = 'Notifications and Listings'
            sheet_name = 'BSE_Listing'
            sh = gc.open(spreadsheet_name).worksheet(sheet_name)
            print(">>> Connected to Sheet")

            # existing_data_range = sh.range('A2:L' + str(sh.row_count))
            # for cell in existing_data_range:
            #     cell.value = ''

            # sh.update_cells(existing_data_range)
            # print(">>> Existing data has been Deleted ")

            df = pd.DataFrame(Data)
            BSE_Listing = df[['NEWS_DT','SCRIP_CD','NEWSSUB','SLONGNAME','NSURL','SUBCATNAME']]
            print(BSE_Listing)
            New_listingNew = r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings\BSE_Listing1.csv'
            if os.path.exists(New_listingNew):
                os.remove(New_listingNew)
            New_listingOld = r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Notifications and Listings\BSE_Listing2.csv'
            BSE_Listing.to_csv(New_listingNew,index=False)
            hash_new = hashlib.sha256(open(New_listingNew,'rb').read()).hexdigest()
            with open(New_listingNew,'rb') as file:
                hash_new = hashlib.sha256(file.read()).hexdigest()
                file.close()
            hash_old = None
            if os.path.exists(New_listingOld):
                with open(New_listingOld,'rb') as f:
                    hash_old = hashlib.sha256(f.read()).hexdigest()
                    f.close()
                if hash_new != hash_old:
                    os.remove(New_listingOld)
                    os.rename(New_listingNew,New_listingOld)

                    existing_data_range = sh.range('A2:L' + str(sh.row_count))
                    for cell in existing_data_range:
                        cell.value = ''

                    sh.update_cells(existing_data_range)
                    print(">>> Existing data has been Deleted ")

                    values_lists = []
                    FormateDate = [i[:10] for i in BSE_Listing.loc[:,'NEWS_DT']]
                    BSE_Listing['NEWS_DT'] = FormateDate
                    BSE_Listing.loc[:,'NEWS_DT'] = pd.to_datetime(BSE_Listing['NEWS_DT'])
                    df_dict = BSE_Listing.to_dict(orient='records')
                    print(f">>> Number Rows to be Updated are: {len(df_dict)}")
                    for data in df_dict:
                        values_lists.append([data['NEWS_DT'].strftime('%Y-%m-%d'),str(data['SCRIP_CD']), str(data['SLONGNAME']), str(data['NSURL']),str(data['SUBCATNAME'])])
                    sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
                    sh.append_rows(values_lists,value_input_option='USER_ENTERED')
                    print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists)}")

                    html_table = "<div style='text-align: center;'><h2>BSE Listing</h2></div>" + BSE_Listing.to_html(index=False)

                    # Step 3: Compose Email
                    with open(r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Keys and Passwords\EmailandPassword\EmailCreds.json") as config_file:
                       config = json.load(config_file)
                    sender_email = config['email_username']
                    receiver_emails = config['email recievers']
                    password = config['email_password']

                    msg = MIMEMultipart('alternative')
                    msg['From'] = sender_email
                    msg['To'] = ', '.join(receiver_emails)
                    msg['Subject'] = 'BSE New Listing'

                    # Step 4: Attach HTML Tables
                    msg.attach(MIMEText(html_table, 'html'))

                    # Step 5: Send Email
                    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                        server.login(sender_email, password)
                        server.sendmail(sender_email, receiver_emails, msg.as_string())

                    print("Email sent successfully!")
                    time.sleep(5)
                else:
                    os.remove(New_listingNew)
                    print('>>> No New Listing ******')
            else:
                os.rename(New_listingNew,New_listingOld)
            # values_lists = []
            # FormateDate = [i[:10] for i in FromPc_df.loc[:,'NEWS_DT']]
            # FromPc_df['NEWS_DT'] = FormateDate
            # FromPc_df.loc[:,'NEWS_DT'] = pd.to_datetime(FromPc_df['NEWS_DT'])
            # df_dict = FromPc_df.to_dict(orient='records')
            # print(f">>> Number Rows to be Updated are: {len(df_dict)}")
            # for data in df_dict:
            #     values_lists.append([data['NEWS_DT'].strftime('%Y-%m-%d'),str(data['SCRIP_CD']), str(data['SLONGNAME']), str(data['NSURL']),str(data['SUBCATNAME'])])
            # sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
            # sh.append_rows(values_lists,value_input_option='USER_ENTERED')
            # print(f">>> New BSE Listing has been updated in the Sheet, Rows = {len(values_lists)}")
            time.sleep(5)

            break
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Retrying the script...")
