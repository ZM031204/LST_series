from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst_timeseries import create_series
from fetch_drive import download_and_clean, check_task_status
from dotenv import load_dotenv
import os
import ee

def __main__():
    load_dotenv()
    SAVE_PATH = os.getenv('SERIES_SAVE_PATH')
    FOLDER_ID = os.getenv('SERIES_FOLDER_ID')

    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # 首次运行需要浏览器授权
    drive = GoogleDrive(gauth)

    ee.Initialize(project='ee-channingtong')
    lat = 114.35
    lon = 30.35
    task = create_series(lat,lon)
    is_success = check_task_status(task, gap = 10)
    if is_success:
        download_and_clean(drive,FOLDER_ID, SAVE_PATH)

if __name__ == '__main__':
    __main__()