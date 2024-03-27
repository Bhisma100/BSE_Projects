from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import os
import pandas as pd
import time

df = pd.read_csv(r"C:\Users\Ashish Pal\Desktop\BSE_Stock_Price\4001to5000.csv",encoding='latin1')
# BSECode = [str(i) [0:len(str(i))-2] for i in df.loc[:,'BSE Code']]
BSECode = df['BSE Code'].to_list()
Merge_Files = []
# Navigate to the website
url = 'https://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.html?flag=0'
# header ={
#         'scheme':'https',
#         'Accept':'application/json, text/plain, */*',
#         'Accept-Language':'en-US,en;q=0.8',
#         'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"',
#         'Sec-Ch-Ua-Mobile': '?0',
#         'Sec-Ch-Ua-Platform':'"Windows"',
#         'Sec-Fetch-Mode':'cors',
#         'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
#         }
# chrome_options = Options()
# chrome_options.add_argument('--headless')
# for key, value in header.items():
#     chrome_options.add_argument(f"--{key}={value}")
# driver = webdriver.Chrome(options = chrome_options)  
driver= webdriver.Chrome()  
driver.get(url)
Not_Found = 0
Found = 0
Counter = 0
# Enter the stock code in the input field

for i in range(0,len(BSECode)):
    stock_code = str(BSECode[i])
    Counter += 1
    input_field = driver.find_element(By.ID, 'scripsearchtxtbx')
    input_field.clear()
    time.sleep(1)
    for char in stock_code:
        input_field.send_keys(char)
        time.sleep(0.1)

    # Wait for the suggestion box to appear
    wait = WebDriverWait(driver, 10)
    suggestion_box = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'quotemenu')))
    # Click on the suggestion that matches the entered stock code
    time.sleep(1)
    # no_match = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="ulSearchQuote2"]/li/a[contains(text(), "No Match Found")]')))
    # if no_match:
    #     print('no match element')
    #     continue
    # else:
    try:
        matching_suggestion = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='ulSearchQuote2']/li"))) 
        matching_suggestion.click()
        time.sleep(1)
        Date_element=driver.find_element(By.XPATH,'//*[@id="rdbDaily"]')
        ActionChains(driver).click(Date_element).perform()
        driver.find_element(By.ID,'txtFromDate').click()
        time.sleep(1)
        try:
            year_1990_option =driver.find_element(By.XPATH,(f'//*[@id="ui-datepicker-div"]/div/div/select[2]/option[text()= "1990"]'))
            year_1990_option.click()
        except NoSuchElementException:
            Years = [2014,2004,1994,1990]
            for i in Years:
                yearBox = wait.until(EC.element_to_be_clickable((By.CLASS_NAME,'ui-datepicker-year')))
                yearBox.click()
                time.sleep(1)
                Year = driver.find_element(By.XPATH,(f'//*[@id="ui-datepicker-div"]/div/div/select[2]/option[text()= "{i}"]'))
                Year.click()
        driver.find_element(By.CLASS_NAME,'ui-datepicker-month').click()
        driver.find_element(By.XPATH,'//*[@id="ui-datepicker-div"]/div/div/select[1]/option[text()= "Jan"]').click()
        driver.find_element(By.XPATH,'//a[text()="1"]').click()
        driver.find_element(By.ID,'txtToDate').click()
        wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="ui-datepicker-div"]/table/tbody/tr[1]/td[6]/a[text()="1"]'))).click()
        submitButton = driver.find_element(By.ID,'btnSubmit')
        ActionChains(driver).click(submitButton).perform()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="lnkDownload"]').click()
        time.sleep(5)
        temp_df = pd.read_csv(rf'C:\Users\Ashish Pal\Downloads\{stock_code}.csv',encoding='latin1')
        BSE_Code = [int(f'{stock_code}')]*len(temp_df.index)
        temp_df.insert(0,'BSE_Code',BSE_Code)   
        os.remove(rf'C:\Users\Ashish Pal\Downloads\{stock_code}.csv')
        Merge_Files.append(temp_df)
        Main_Data = pd.concat(Merge_Files,ignore_index=True)
        Found += 1
        print(f"Stock Count = {Counter} and Stock Code = {stock_code} and Found on BSE")
    except Exception as e:
        # print(f"Error= {e}")
        Not_Found += 1
        print(f"Stock Count = {Counter} and Stock Code = {stock_code} and  Not Found on BSE")
        continue

driver.close()
Main_Data.to_csv(r'C:\Users\Ashish Pal\Desktop\BSE_Stock_Price\BSE_Stock_Price4001to5000.csv')
print(F"Total stocks whose price Found On BSE is {Found}\nand  no of Stock who were not available on BSE is {Not_Found}")
# main_df = pd.concat(Merge_Files,ignore_index=True)
# main_df.to_csv('Entities1to1000')
#[contains(text(), '{stock_code}')]
# Continue with further actions as needed

