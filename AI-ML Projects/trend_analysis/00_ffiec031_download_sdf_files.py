# Load packages
import os
import re
import time
from datetime import datetime, date, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# Function to generate the quarter end dates starting from 03312001 to most recent quarter-end date
def generate_quarter_end_dates():
    start_date = datetime(2001, 3, 31)
    today = datetime.today()
    quarter_end_dates = []
    current_date = start_date

    while current_date <= today:
        quarter_end_dates.append(current_date.strftime('%m%d%Y'))
        if current_date.month == 3:
            current_date = datetime(current_date.year, 6, 30)
        elif current_date.month == 6:
            current_date = datetime(current_date.year, 9, 30)
        elif current_date.month == 9:
            current_date = datetime(current_date.year, 12, 31)
        else:  # December
            current_date = datetime(current_date.year + 1, 3, 31)

    return quarter_end_dates


# Function to get the most recent quarter end date
def get_recent_quarter_end():
    today = date.today()
    year = today.year
    # Define quarter-end months in reverse order
    quarter_end_months = [12, 9, 6, 3]

    # Find the most recent valid quarter-end date
    for month in quarter_end_months:
        # Get the last day of the month dynamically
        next_month_first = date(year, month % 12 + 1, 1) if month != 12 else date(year + 1, 1, 1)
        quarter_end = next_month_first - timedelta(days=1)

        # Check if quarter-end is in the past or today
        if quarter_end <= today:
            return quarter_end

    # If all quarter-ends are in the future, check last year's last quarter-end
    return date(year - 1, 12, 31)


# Function to download the SDF file for a given quarter end date
def download_sdf(quarter_end_date, download_folder):
    url = f"https://cdr.ffiec.gov/Public/ViewFacsimileDirect.aspx?ds=call&idtype=id_rssd&id=480228&date={quarter_end_date}"
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Initialize Chrome WebDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)
        time.sleep(5)  # Wait for the page to load
        download_button = driver.find_element(By.ID, "Download_SDF_3")
        download_button.click()
        time.sleep(10)  # Wait for download
        print(f"File for {quarter_end_date} downloaded successfully to {download_folder}")
    except Exception as e:
        print(f"Error downloading file for {quarter_end_date}: {e}")
    finally:
        driver.quit()


# Function to list all files in a download folder
def list_files_in_folder(download_folder):
    files = [f for f in os.listdir(download_folder) if os.path.isfile(os.path.join(download_folder, f))]
    return files


# Function to create a list of sdf files to download at time of script run
def create_sdf_filenames():
    ls_filenames = []
    quarter_end_dates = generate_quarter_end_dates()
    for quarter_end_date in quarter_end_dates:
        quarter_end_date_short = quarter_end_date[:4]+quarter_end_date[-2:]
        filename = f"Call_Cert3510_{quarter_end_date_short}.SDF"
        ls_filenames.append(filename)
    return ls_filenames


# Function to compare existing sdf files against sdf files to download
def find_sdf_files_to_download(download_folder):
    ls_files_in_folder = list_files_in_folder(download_folder)
    ls_files_to_download = create_sdf_filenames()
    ls_files_not_in_folder = [file for file in ls_files_to_download if file not in ls_files_in_folder]
    return ls_files_not_in_folder

# Function to obtain quarter end dates to download
def get_quarter_end_dates_to_download(download_folder):
    count_files_to_download = len(create_sdf_filenames())
    sdf_files_to_download = find_sdf_files_to_download(download_folder)

    ls_quarter_end_dates_to_download = []
    if sdf_files_to_download:
        for sdf_file in sdf_files_to_download:
            quarter_end_date = sdf_file.split('_')[-1].replace('.SDF', '')
            quarter_end_date = quarter_end_date[:4]+'20'+quarter_end_date[-2:]
            ls_quarter_end_dates_to_download.append(quarter_end_date)

    count_missing_files = len(sdf_files_to_download)
    if count_missing_files > 0:
        print(f"Out of a total of {count_files_to_download} file(s), {count_missing_files} file(s) could not be found in the download folder.")
        print(f"The following file(s) need(s) to be downloaded: {sdf_files_to_download}")
    else:
        print(f"Out of a total of {count_files_to_download} file(s), all files are found in the download folder.")
    return ls_quarter_end_dates_to_download


# Function to find the most recent quarter end dates in the download folder
def find_most_recent_quarter_end_date(download_folder):
    ls_files_in_folder = list_files_in_folder(download_folder)
    ls_quarter_end_dates = []
    for file in ls_files_in_folder:
        pattern = r'_(\d{6})\.'
        match = re.search(pattern, file)
        if match:
            result = match.group(1)
            result_reformat = '20'+ result[-2:] + result[:4]
            ls_quarter_end_dates.append(result_reformat)
    return max(ls_quarter_end_dates)


# Main function to orchestrate the download process
def main():
    download_folder = "C:\\Users\\james\\PycharmProjects\\data-analysis-portfolio\\AI & ML Projects\\inputs\\ffiec_031_sdf_files"
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    most_recent_quarter_end_date = find_most_recent_quarter_end_date(download_folder)
    quarter_end_dates = get_quarter_end_dates_to_download(download_folder)

    if quarter_end_dates:
        for quarter_end_date in quarter_end_dates:
            print(f"Attempting to download SDF for the quarter end date: {quarter_end_date}")
            download_sdf(quarter_end_date, download_folder)
    else:
        print(f"No SDF files to download - most recent quarter-end date is: {most_recent_quarter_end_date}")


# Execute the main function
if __name__ == "__main__":
    main()
