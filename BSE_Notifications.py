import gspread
from requests_html import HTMLSession
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import datetime

print('************** BSE Notification Batch *******************')
s = HTMLSession()
url = 'https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx'
r = s.get(url)
r.html.arender(sleep=5)

df = pd.DataFrame(columns=['Notice_Number','Subject','Segment Name','Category Name','Department'])
if r.status_code == 200:
    print(">>> Connection Succesfull")
    table = r.html.xpath('//*[@id="divmain"]/div[4]/div/div',first = True)
    soup = bs(table.html,'lxml')
    table = soup.find('table', {'id': 'ContentPlaceHolder1_GridView1'})
    Master_Data = []
    if table:
        # Extract data from the table
        rows = table.find_all('tr')
        for row in rows[1:]:  # Skip the header row (index 0)
            columns = row.find_all(['td'])
            row = [tr.text for tr in columns]
            filter_row = row[:5]
            l = len(df)
            df.loc[l] = filter_row


    # Getting Links 
    Notifications_Links = []
    table = r.html.xpath('//*[@id="divmain"]/div[4]/div/div',first = True)
    soup = bs(table.html,'lxml')
    table = soup.find('table', {'id': 'ContentPlaceHolder1_GridView1'})
    links = table.find_all('a',{'class': 'tablebluelink'})
    for href_l in links:
        href_p = href_l.get('href')
        Notifications_Links.append("www.bseindia.com"+href_p)
    df['Notification_link'] = Notifications_Links
    
    # Connecting to the GoogleSheet
    gc = gspread.service_account(filename=r'C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Exchange Related task\BSE_Projects\creds.json')
    spreadsheet_name = 'Notifications and Listings'
    sheet_name = 'BSE_Forthcoming_listing'
    sh = gc.open(spreadsheet_name).worksheet(sheet_name)
    print(">>> Connected to Sheet")
    # Existing Data Check
    if r.status_code == 200:
        existing_data_range = sh.range('A2:F' + str(sh.row_count))
        for cell in existing_data_range:
            cell.value = ''

        sh.update_cells(existing_data_range)
        print(">>> Existing data has been deleted")
    else :
        pass

    Filtered_df = df[df ['Subject'].str.contains('Listing of Equity|Listing of Units')& df['Category Name'].str.contains('Company related')]
    
    #Inderting the data in sheet
    values_lists = []
    df_dict = Filtered_df.to_dict(orient='records')
    for data in df_dict:
        values_lists.append([str(data['Notice_Number']), str(data['Subject']), str(data['Segment Name']),str(data['Category Name']),str(data['Notification_link'])])
    sh.append_row([f'Batch ran at: {datetime.datetime.now()}'])
    sh.append_rows(values_lists)
    print(">>> Data has been Updated in the sheet")
    time.sleep(2)
else:
    print(f'Response Code: {r.status_code}')