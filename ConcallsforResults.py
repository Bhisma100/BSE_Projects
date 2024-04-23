import requests
import pandas as pd
import gspread
from datetime import timedelta,datetime
import time

print('**************************************************  Concall Batch *****************************************************')
s = requests.Session()
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

# Date = (datetime.now()-timedelta(days=1)).strftime('%Y%m%d')
Date = datetime.now().strftime('%Y%m%d')

#Connectiong to the GoogleSheet
print(">>> Connecting to Google Sheet")
GoogleCreds = gspread.service_account(filename=r"C:\Users\Ashish Kumar Pal\OneDrive\Desktop\Python\Keys and Passwords\GoogleCloud(Key)\creds.json")
SpreaSheetName = 'QuarterlyResultsTracker'
WorksheetName = 'ConcallAnnoucements'
sh = GoogleCreds.open(SpreaSheetName).worksheet(WorksheetName)
# Deleting Existing Data
print(">>> Connection Sucessfull ")

pageno = 1
FinalNotificatioData = [["Batch ran at:", datetime.now().strftime('%d/%m/%Y, %H:%M:%S')]]
CorporateNotifications = []
while True:
    print(f'>>> Extrcting Data From page no:{pageno}')
    url = f'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno={pageno}&strCat=-1&strPrevDate={Date}&strScrip=&strSearch=P&strToDate={Date}&strType=C&subcategory='
    r = s.get(url,headers=header)
    if len(r.json()['Table']) > 0:
        NotiTable = r.json()['Table']
        TempCorpNotiDF = pd.DataFrame(NotiTable)
        CorporateNotifications.append(TempCorpNotiDF)
        pageno += 1
    else:
        break


MasterData = pd.concat(CorporateNotifications,axis=0,ignore_index=True)

ConCallIntimationData = MasterData[MasterData['HEADLINE'].str.contains('Earnings Call|Earning Conference Call|intimation for conference call|Con-call|intimation of Schedule|Intimation of the Earnings Call|Earning Call',case= False)|MasterData['MORE'].str.contains('Earnings Call|Earnings Conference Call|Con-Call|Earning Call',case= False)|MasterData['NEWSSUB'].str.contains('Earnings Call|Earning Conference Call|Con-Call|intimation for conference call|Earning Call',case= False)]
ConCallIntimationData = ConCallIntimationData[['SCRIP_CD','SLONGNAME','NEWS_DT','NEWSSUB','ATTACHMENTNAME']]
ConCallIntimationData['ATTACHMENTNAME'] = ConCallIntimationData['ATTACHMENTNAME'].apply(lambda x: f'https://www.bseindia.com/xml-data/corpfiling/AttachLive/{x}')
ConCallIntimationData['NEWS_DT'] = pd.to_datetime(ConCallIntimationData['NEWS_DT'])
print(ConCallIntimationData)


existing_data_range = sh.range('A2:L' + str(sh.row_count))
for cell in existing_data_range:
    cell.value = ''

sh.update_cells(existing_data_range)

DataDict = ConCallIntimationData.to_dict(orient='records')
for Data in DataDict:
    date_str = Data['NEWS_DT'].strftime("%d-%b-%Y")
    FinalNotificatioData.append([int(Data['SCRIP_CD']),str(Data['SLONGNAME']),date_str,str(Data['NEWSSUB']),str(Data['ATTACHMENTNAME'])])
time.sleep(1)
sh.append_rows(FinalNotificatioData,value_input_option='USER_ENTERED')
print(">>> Data Has been Updated in the sheet ")
time.sleep(3)