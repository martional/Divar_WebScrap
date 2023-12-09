import time
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import numpy as np
import pandas as pd
import openpyxl
from unidecode import unidecode
from arabic_reshaper import reshape
import progressbar
import time
import os
import traceback
from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file
import uuid
import threading
import jdatetime
import logging





# save the urls of all advertisements
# Web scrapper for infinite scrolling page #
def save_urls(scroll_times, url_file, province_name,append,data_file,excel_file,username,try_num,expire_date):
    home_url = 'https://divar.ir'
    cities = {
        'alborz-province': ["2","774","850","1720","1721","1722","1738","1739","1740","1751","1752","1753","1754"],
        'gilan-province': ["12","708","746","824","825","826","827","828","829","860","861","862","863","864","1683",
            "1684","1686","1687","1688","1689","1690","1809","1810","1811","1812","1813","1814","1815","1834","1835",
            "1836","1837","1839","1840","1841","1842","1843","1844","1845","1846","1847","1848","1849","1850","1851",
            "1852","1853","1854","1855"],
        'tehran-province': ["1","29","781","782","783","784","1706","1707","1708","1709","1710","1711","1712","1713",
            "1714","1715","1716","1717","1718","1719","1758","1759","1760","1761","1762","1763","1764","1765","1766",
            "1767","1768","1769","1770","1771","1772"],
        'mazandaran-province': ["22","663","664","665","709","710","744","745","832","833","834","835","836","837",
            "838","1694","1695","1696","1697","1698","1699","1700","1701","1702","1703","1818","1819","1856","1858",
            "1859","1860","1861","1862","1863","1864","1865","1866","1867","1868","1869","1870","1871","1872","1873",
            "1874","1875","1876"]
    }
# progress bar
    bar1 = progressbar.ProgressBar(maxval= scroll_times, \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])  
    index_counter = 0   
    print('grab links progress:')
    bar1.start() 


    with open(url_file, 'w', newline='', encoding='utf-8') as write_obj:
                    write_obj.writelines('')
            
    # Initialize variables
    list_of_tokens = []
    count = 0

    # Base URL for the initial GET request
    url_base = 'https://api.divar.ir/v8/web-search/'+ province_name + '/buy-residential'

    # Try to make the initial GET request
    try:
        res = requests.get(url_base)
        # Check if the request was successful
        if res.status_code == 200:
            data_base = res.json()
            last_post_date = data_base['last_post_date']
            # Process each post in the initial response
            for post in data_base['web_widgets']['post_list']:
                try:
                    token = post['data']['token']
                    ad_date = post['data']['bottom_description_text']
                    #if(ad_date)
                    list_of_tokens.append(token)
                    count += 1
                    bar1.update(index_counter+1)
                    index_counter += 1
                except:
                    logging.exception("Exception occurred")
                    pass
                
        else:
            print("Failed to retrieve data: Status code", res.status_code)
            logging.warning("Failed to retrieve data: Status code", res.status_code)
            exit()
    except Exception as e:
        print("An error occurred:", e)
        logging.exception("Exception occurred")
        exit()

    # URL for subsequent POST requests
    url_next = 'https://api.divar.ir/v8/web-search/1/residential-sell'
    headers = {
        "Content-Type": "application/json"
    }

    # Continue fetching data until limit is reached
    while count < scroll_times:
        
        json_payload = {
            "json_schema": {"category": {"value": "real-estate"}},
            "last-post-date": last_post_date,
            "cities": cities[province_name]
        }

        # Try to make the POST request
        try:
            res = requests.post(url_next, json=json_payload, headers=headers)

            # Check if the request was successful
            if res.status_code == 200:
                data = res.json()
                last_post_date = data['last_post_date']
                # Process each post in the response
                for post in data['web_widgets']['post_list']:
                    try:
                        token = post['data']['token']
                        url = urljoin(home_url+'/v/-/', token)
                        # find the rent urls and save in the text file
                        with open(url_file, 'a+', newline='', encoding='utf-8') as write_obj:
                            write_obj.writelines(url + '\n')
                        count += 1
                        bar1.update(index_counter+1)
                        index_counter += 1
                    except:
                        logging.exception("Exception occurred")
                        print("token is empty")

                    # Break if limit is reached
                    if count >= scroll_times:
                        break
                    
            else:
                print("Failed to retrieve data: Status code", res.status_code)
                logging.warning("Failed to retrieve data: Status code", res.status_code)
                time.sleep(5)
        except Exception as e:
            print("An error occurred during POST request:", e)
            logging.exception("Exception occurred")
            time.sleep(5)
        time.sleep(1)

    # Write the tokens to a file
    '''for token in list_of_tokens:
        url = urljoin(home_url+'/v/-/', token)
        # find the rent urls and save in the text file
        with open(url_file, 'a+', newline='', encoding='utf-8') as write_obj:
            write_obj.writelines(url + '\n')'''
    bar1.finish()
    print("Data collection complete. Tokens written to tokens.txt")
    logging.warning("Data collection complete. Tokens written to tokens.txt")
    scrap_links(append, url_file, data_file, username, excel_file,try_num,province_name)


# scrap all links in url_file
def scrap_links(append=False, url_file="", data_file="", username="", excel_file="",try_num=10, province_name=""):
    data_list = []
    with open(url_file, 'r', newline='', encoding='utf-8') as read_obj:
        links = read_obj.readlines()
        
        print('------------------------------')
        print('Total link counts:',len(links))
        logging.warning('Total link counts:',len(links))
        # remove duplicates
        links =  list(set(links))
        print('Unique link counts:',len(links))
        print('------------------------------')

    # Write the headers in data csv file
    '''with open(data_file, mode='w', newline='', encoding='utf-8') as csv_file:
        handle = csv.writer(csv_file)
        handle.writerow(['neighborhood','area','year','room','deposit','rent','floor'
            ,'elavator','parking','warehouse', 'link'])'''
    
    
    # progress bar
    bar1 = progressbar.ProgressBar(maxval= len(links), \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    
    index_counter = 0
    
    print('Scraping links progress:')
    logging.warning('Scraping links progress:')

    
    bar1.start()
    
    for each_link in links:
        time.sleep(1)
        bar1.update(index_counter+1)
        index_counter += 1 
        each_link = each_link.replace('\n','')
        neighborhood = area = year = room = deposit = rent = floor = ''
        elavator = parking = warehouse = city= post_type = post_time = ''
        # Base URL for the initial GET request
        url_post = 'https://api.divar.ir/v8/posts-v2/web/' + each_link[-8:]

        # Try to make the initial GET request
        try:
            res = requests.get(url_post)
            # Check if the request was successful
            if res.status_code == 200:
                data = res.json()
                # Process each post in the initial response
                try:
                    neighborhood = data['seo']['web_info'].get('district_persian')
                    city = data['seo']['web_info'].get('city_persian')
                    post_type = data['seo']['web_info'].get('category_slug_persian')
                    if post_type != 'پیش‌فروش ملک':
                        # Search for the dictionary with "section_name" equal to "LIST_DATA"
                        desired_section = None
                        for section_data in data['sections']:
                            if section_data.get("section_name") == "TITLE":
                                post_time = section_data['widgets'][0]['data'].get('subtitle')
                            if section_data.get('section_name') == "LIST_DATA":
                                desired_section = section_data
                                for info in desired_section['widgets']:
                                    if info.get('widget_type') == "GROUP_INFO_ROW":
                                        area = info['data']['items'][0].get('value')
                                        year = info['data']['items'][1].get('value')
                                        if len(info['data']['items']) > 2:
                                            room = info['data']['items'][2].get('value')
                                        
                                    if info.get('widget_type') == "GROUP_FEATURE_ROW":
                                        elavator = info['data']['items'][0].get('title')
                                        parking = info['data']['items'][1].get('title')
                                        warehouse = info['data']['items'][2].get('title')
                                        if warehouse == 'بالکن':
                                            warehouse = ''
                                    if info['data'].get('title') == "قیمت کل":
                                        deposit = info['data'].get('value')
                                    if info['data'].get('title') == "قیمت هر متر":
                                        rent = info['data'].get('value')
                                    if info['data'].get('title') == "طبقه":
                                        floor = info['data'].get('value')


                    if desired_section == None:
                        print ("failed")
                        logging.warning("failed")
                        break                    
                    #balcony = desired_section['widgets'][7]['data']['action']['payload']['modal_page']['widget_list'][8][data]['disabled']
                    # Create a dictionary for each row
                    new_row = {
                        'neighborhood': neighborhood,
                        'area': area,
                        'year': year,
                        'room': room,
                        'deposit': deposit,
                        'rent': rent,
                        'floor': floor,
                        'elavator': elavator,
                        'parking': parking,
                        'warehouse': warehouse,
                        'city' : city,
                        'post_type' : post_type,
                        'post_time' : post_time,
                        'link': each_link,
                    }
                    data_list.append(new_row)  # Add the dictionary to the list

                except Exception as e:
                    print("An error occurred:", e)
                    logging.exception("Exception occurred")
                    traceback.print_exc()
                    print(each_link)
                    
            else:
                print("Failed to retrieve data: Status code", res.status_code)
                logging.warning("Failed to retrieve data: Status code", res.status_code)
                
        except Exception as e:
            print("An error occurred:", e)
            logging.exception("Exception occurred")
            if try_num:
                time.sleep(5)
                try_num = try_num - 1
                scrap_links(append, url_file, data_file, username, excel_file,try_num)

    # Check if CSV file exists and if it is empty
    file_exists = os.path.isfile(data_file) and os.path.getsize(data_file) > 0

    # Open the CSV file in append mode if append is True, else in write mode
    csv_mode = 'a+' if append and file_exists else 'w'

    with open(data_file, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write headers only if file does not exist or is empty
        if not file_exists or not append :
            csv_writer.writerow(['neighborhood', 'area', 'year', 'room', 'deposit', 'rent', 'floor',
                                'elavator', 'parking', 'warehouse', 'city', 'post_type', 'post_time', 'link'])

        # Write data to CSV file
        for row in data_list:
            new_row = [row['neighborhood'], row['area'], row['year'], row['room'], row['deposit'],
            row['rent'], row['floor'], row['elavator'], row['parking'], row['warehouse'],
            row['city'], row['post_type'], row['post_time'], row['link']]
            with open(data_file, 'a+', newline='', encoding='utf-8') as write_obj:
                # Create a writer object from csv module
                csv_writer = csv.writer(write_obj)
                # Add contents of list as last row in the csv file\n",
                csv_writer.writerow(new_row)

        # Handling Excel file
        if append and os.path.isfile(excel_file):
            existing_df = pd.read_excel(excel_file)
            new_df = pd.DataFrame(data_list)
            df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            df = pd.DataFrame(data_list)

    # Write DataFrame to Excel
    df.to_excel(excel_file, index=False)
            # Write data refer to user
    write_username_file(username, data_file, province_name, len(data_list))
    bar1.finish()

# change farsi characters and clean data set
def check_for_failed_link():
    # Read the CSV file
    df = pd.read_csv('Data.csv', encoding='utf-8')

    # Identify rows with missing data in specified columns
    # Assuming 'neighborhood' column will always have data, so starting check from 'area'
    columns_to_check = ['area', 'year', 'room', 'deposit', 'rent', 'floor', 'elavator', 'parking', 'warehouse',
                         'city', 'post_type', 'post_time', 'link']
    incorrect_rows = df[df[columns_to_check].isnull().all(axis=1)]

    # Extract the links from these rows
    links = incorrect_rows['link'].tolist()

    # Write these links to a text file
    with open('AdsUrl.txt', 'w', encoding='utf-8') as file:
        for link in links:
            file.write(link + '\n')

    print(f'Links of incorrect entries are written to incorrect_entries_links.txt')
    scrap_links(append=True)


# change farsi characters and clean data set
def clean_data():
    df = pd.read_csv('Data.csv', encoding="cp1256")  
    df.drop_duplicates(subset =None, keep = 'first', inplace = True)
    # filter apartments
    # چون فقط آپارتمانها را می گیریم، بالکن ندارند
    df = df[df['neighborhood'].str.contains('اجاره آپارتمان')]
    df['neighborhood'] = df['neighborhood'].astype(pd.StringDtype())

    # چون ستون طبقه، دارای مقادیر زیادی از نال و موارد نادرست است آن را حذف می کنیم
    df.drop('floor', inplace=True, axis=1)
    # ستون بالکون برای تمام موارد نال است
    df.drop('balcony', inplace=True, axis=1)

    # int columns
    df['area'] = pd.to_numeric(df.area.apply(unidecode), errors='coerce').replace(np.nan, 0).astype(int)
    df['room'] = pd.to_numeric(df.room.apply(unidecode), errors='coerce').replace(np.nan, 0).astype(int)

    # string columns
    df['warehouse'] = df['warehouse'].replace({'انباری ندارد': '۰'}, regex=True)
    df['warehouse'] = df['warehouse'].replace({'انباری': '۱'}, regex=True)
    df['warehouse'] = pd.to_numeric(df['warehouse'].apply(unidecode), errors='coerce').replace(np.nan, 0).astype(int)

    df['elavator'] = df['elavator'].replace({'آسانسور ندارد': '۰'}, regex=True)
    df['elavator'] = df['elavator'].replace({'آسانسور': '۱'}, regex=True)
    df['elavator'] = pd.to_numeric(df['elavator'].apply(unidecode), errors='coerce').replace(np.nan, 0).astype(int)

    df['parking'] = df['parking'].replace({'پارکینگ ندارد': '۰'}, regex=True)
    df['parking'] = df['parking'].replace({'پارکینگ': '۱'}, regex=True)
    df['parking'] = pd.to_numeric(df['parking'].apply(unidecode), errors='coerce').replace(np.nan, 0).astype(int)

    #df[['neighborhood','temp1']] = df['neighborhood'].str.split('،',expand=True)
    #df[['temp1','temp2']] = df['temp1'].str.split('|',expand=True)
    #df['neighborhood'] = df['temp1'].replace({'‌': ' '}, regex=True)

    df['deposit'] = df['deposit'].replace({'مجانی': '۰'}, regex=True)
    df['deposit'] = df['deposit'].replace({'توافقی': '۰'}, regex=True)
    df['deposit'] = df['deposit'].replace({'٫': ''}, regex=True)
    df['deposit'] = df['deposit'].replace({'تومان': ''}, regex=True)
    df['deposit'] = pd.to_numeric(df['deposit'].apply(unidecode), errors='coerce').replace(np.nan, 0).astype(float)

    df['rent'] = df['rent'].replace({'مجانی': '۰'}, regex=True)
    df['rent'] = df['rent'].replace({'توافقی': '۰'}, regex=True)
    df['rent'] = df['rent'].replace({'٫': ''}, regex=True)
    df['rent'] = df['rent'].replace({'تومان': ''}, regex=True)
    df['rent'] = pd.to_numeric(df['rent'].apply(unidecode), errors='coerce').replace(np.nan, 0).astype(float)

    # قبل از 1370 را با 1363 پر می کنم تا فاصله ها حفظ شود
    df['year'] = df['year'].replace({'قبل از ۱۳۷۰': '۱۳۶۳'}, regex=True)
    df['year'] = pd.to_numeric(df.year.apply(unidecode), errors='coerce').replace(np.nan, 0).astype(int)

    # تبدیل اجاره و ودیعه به یکدیگر و به دست آوردن یک عدد به عنوان ارزش منزل
    #df['total_value'] = ((df['rent'] * 3) / 100) + df['deposit']

    # remove temp columns
    #df.drop(columns = ['temp1','temp2'], inplace=True, axis=1)

    df = df[['neighborhood', 'area', 'year', 'room', 'deposit', 'rent', 'floor',
                                'elavator', 'parking', 'warehouse']]
    
    df.to_csv(r'Data1.csv', index = False) 

    # File path of your text file

# File path of your text file
file_path = 'path_to_your_file.txt'

# Function to find the last record for a given username
def find_last_record(username):
    last_record = None
    with open('user_files.txt', 'r') as file:
        for line in file:
            if line.startswith(username + ":"):
                # Splitting the line and getting the part after 'username:'
                last_record = line.strip().split(':')[1]
    return last_record

def generate_unique_filename(base_path):
    # Get the current timestamp
    current_time = int(time.time())

    # Generate a random string (UUID4)
    random_string = str(uuid.uuid4().hex)[:8]

    # Combine timestamp, random string, and the desired extension
    unique_txt = f"{current_time}_{random_string}.{'txt'}"
    unique_csv = f"{current_time}_{random_string}.{'csv'}"
    unique_xlsx = f"{current_time}_{random_string}.{'xlsx'}"

    # Check if the file already exists, if so, generate a new one
    while os.path.exists(os.path.join(base_path, unique_txt)):
        random_string = str(uuid.uuid4().hex)[:8]
        unique_txt = f"{current_time}_{random_string}.{'txt'}"
        unique_csv = f"{current_time}_{random_string}.{'csv'}"
        unique_xlsx = f"{current_time}_{random_string}.{'xlsx'}"

    return unique_txt, unique_csv, unique_xlsx 

def write_username_file(username, file_name, province_name, recoed_count):
    with open("user_files.txt", 'a+', newline='', encoding='utf-8') as write_obj:
        write_obj.writelines(f'{username}:{file_name}:{province_name}:{recoed_count}:{current_time()}\n') 

def current_time():
    # Get the current Jalali date and time
    jalali_datetime = jdatetime.datetime.now()
    
    # Format the date and time in a specific format
    formatted_datetime = jalali_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    return formatted_datetime


def scrap(province_name='mazandaran-province', count=48,expire_date='۱ ساعت'):

    url_file, data_file, excel_file = generate_unique_filename("/")
    session['search_thread_1_urls'] = url_file
    session['search_thread_1_data_file'] = data_file
    session['search_thread_1_stage'] = 'prestart'
    session['search_thread_1_count'] = 0
    session['search_thread_1_last_try'] = 0
    session['search_thread_1_permision'] = True


    username = ''
    if 'username' in session:
        username = session['username']
    append = False
    
    try_num = 10
    # 1- save the urls of advertisements in a file
    #save_urls(24, url_file, province_name)
    save_urls(count, url_file, province_name,append,data_file,excel_file,username,try_num,expire_date)
    #thread_save_urls = threading.Thread(target=save_urls, args=(count, url_file, province_name,append,data_file,
                                                                #excel_file,username,try_num,expire_date))
    #thread_save_urls.start()
    

    # 2- read links from file and scrap all links
    # Create a thread object
    #thread = threading.Thread(target=scrap_links, args=(append, url_file, data_file, username, excel_file))

    # Start the thread
    #thread.start()

    # Optionally, wait for the thread to complete
    #thread.join()
    #scrap_links()

    # 3- rescrap failed link
    #check_for_failed_link()
    
    # 4- change farsi characters and clean data
    #clean_data()

def translate_province(province_name):
    """ Translate English province names to Persian. """
    translations = {
        'tehran-province': 'استان تهران',
        'alborz-province': 'استان البرز',
        'mazandaran-province': 'استان مازندران',
        'gilan-province': 'استان گیلان',
        # Add more translations as needed
    }
    return translations.get(province_name, province_name)


def generate_html_table(username, filename):
    rows = []
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.strip().split(':')
            if parts[0] == username:
                province_persian = translate_province(parts[2])
                row = f"""  <tr>
                            <td>
                            <i class="mdi mdi-20px  me-3"></i>{parts[4]}
                            </td>
                            <td>{province_persian}</td>
                            <td>30</td>
                            <td><span class="badge bg-label-success me-1">تکمیل شده</span></td>
                            <td><div class="dropdown">
                            <button type="button" class="btn p-0 dropdown-toggle hide-arrow" data-bs-toggle="dropdown"><i class="mdi mdi-dots-vertical"></i></button><div class="dropdown-menu">
                            <a class="dropdown-item" href=/download/{parts[1][:-3]}xlsx>Download Excel</a><a class="dropdown-item" href=/download/{parts[1]}>Download CSV</a><a class="dropdown-item" href="javascript:void(0);"><i class="mdi mdi-trash-can-outline me-1"></i>حذف</a>
                            </div>
                            </div></td>
                            </tr>"""
                #row = f"<tr><td>{len(rows) + 1}</td><td>{province_persian}</td><td>{parts[4]}</td><td><a href='{parts[1]}'>Download CSV</a></td></tr>"
                rows.append(row)
    
    if not rows:
        return "No records found for the specified username."

    table_html = f"""
        {''.join(rows)}
    """
    return table_html

app = Flask(__name__, static_url_path='/static')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


# Configure a secret key (for session management and flash messages)
app.secret_key = '"8xp%&ochzn!w#c8y2j+cn21i%#k371yl2g%%t8^j!7w-=h&#"'

# Define a route for the home page
@app.route('/')
def index():
    if 'username' in session:
        return render_template('index.html')
    else:
        return redirect(url_for('login'))   

# Define a route for the History page
@app.route('/history', methods=['GET', 'POST'])
def history():
    if 'username' in session:
        username = session['username']
        html_table = generate_html_table(username, 'user_files.txt')
        return render_template('history.html', table_html=html_table)

    else:
        return redirect(url_for('login'))   

# Define a route for the login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # Check if the provided credentials are valid
        if check_credentials(username, password):
            session['logged_in'] = True
            session['username'] = username
            flash('Login successful!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Invalid username or password. Please try again.', 'danger')

    return render_template('login.html')

# Define a route to handle form submission
@app.route('/submit', methods=['POST'])
def submit():
    # Get user input from the form
    user_input = request.form.get('province')
    count = 30
    expire_date = request.form.get('expire')
    if 'ساعت' in expire_date:
        count=30
    elif 'روز' in expire_date:
        count=300
    elif '۱ هفته' in expire_date:
        count=3000
    elif '۴' in expire_date:
        count=30000
    scrap(user_input, count,expire_date)

    # Implement your Python code here
    # Example: Perform an action with user_input
    # result = your_function(user_input)
    # Generate a sample result (replace this with your actual result)
    result = f"You entered: {user_input}"

    # Create a CSV file with the result
    df = pd.DataFrame({'Result': [result]})
    result_csv_path = 'result.csv'
    df.to_csv(result_csv_path, index=False)

    flash('جمع آوری آگهی از سایت دیوار آغاز شد!', 'success')
    return redirect(url_for('index'))

# Define a route to display the result and provide a download link
@app.route('/result/<result_csv_path>')
def result(result_csv_path):
    if os.path.exists(result_csv_path):
        return render_template('result.html', result_csv_path=result_csv_path)
    else:
        flash('Result not found!', 'danger')
        return redirect(url_for('index'))

# Define a route to display the result and provide a download link
@app.route('/result')
def user_result():
    username = session['username']
    last_record = find_last_record(username)
    if os.path.exists(last_record):
        return render_template('result copy.html', result_csv_path=last_record[:-3])
    else:
        flash(last_record)
        #flash('Result not found!', 'danger')
        return redirect(url_for('index'))

# Define a route to download the result CSV file
@app.route('/download/<result_csv_path>')
def download(result_csv_path):
    return send_file(result_csv_path, as_attachment=True)

# Function to check user credentials
def check_credentials(username, password):
    with open('users.txt', 'r') as users_file:
        for line in users_file:
            stored_username, stored_password = line.strip().split(':')
            if username == stored_username and password == stored_password:
                return True
    return False



# Logout route
@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80,debug=True)
    
    


