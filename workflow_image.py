from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst_image import create_lst_image
from fetch_drive import download_and_clean, check_task_status
from fetch_drive import get_folder_id_by_name as get_folder_id
from dotenv import load_dotenv
import os
import ee
import time
def __main__():
    load_dotenv()
    SAVE_PATH = os.getenv('SERIES_SAVE_PATH')
    SERIES_FOLDER_ID = os.getenv('SERIES_FOLDER_ID')

    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # 首次运行需要浏览器授权
    drive = GoogleDrive(gauth)

    ee.Initialize(project='ee-channingtong')
    year = 2022
    month_list = range(1,13)
    month_list = [10] # for test
    folder_name = 'landsat_lst_timeseries'
    task = create_lst_image(year,month_list,SERIES_FOLDER_ID)
    is_success = check_task_status(task)
    if is_success:
        time.sleep(30) # wait for the last iamge to be created
        folder_id = get_folder_id(drive,folder_name)
        download_and_clean(drive,folder_id, SAVE_PATH)

if __name__ == '__main__':
    __main__()