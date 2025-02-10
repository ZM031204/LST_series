from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst import create_lst_image
from fetch_drive import download_and_clean
from fetch_drive import create_folder
import os
import ee
from datetime import datetime
from dotenv import load_dotenv
import time


def check_task_status(task):
    """
    监控任务状态直到完成或失败
    
    Args:
        task: ee.batch.Task 对象
        timeout_minutes: 超时时间（分钟）
    
    Returns:
        bool: 任务是否成功完成
    """
    
    while True:
        # 获取任务状态
        status = task.status()
        state = status['state']
        
        # 打印当前状态
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 任务状态: {state}")
        
        # 检查是否完成
        if state == 'COMPLETED':
            print("✓ 任务成功完成！")
            return True
            
        # 检查是否失败
        elif state in ['FAILED', 'CANCELLED']:
            error_message = status.get('error_message', '未知错误')
            print(f"× 任务失败: {error_message}")
            return False
            
        # 等待一段时间再检查
        time.sleep(20)  # 10秒检查一次


def __main__():
    load_dotenv()
    FOLDER_ID = os.getenv('FOLDER_ID')
    SAVE_PATH = os.getenv('SAVE_PATH')

    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # 首次运行需要浏览器授权
    drive = GoogleDrive(gauth)

    ee.Initialize(project='ee-channingtong')
    year = 2022
    month_list = range(1,13)
    month_list = [10] # for test
    folder_name = f'LST/{str(year)}'
    # folder_id = create_folder(drive,FOLDER_ID,folder_name)
    task = create_lst_image(year,month_list,folder_name)
    is_success = check_task_status(task)
    if is_success:
        download_and_clean(drive,folder_name, SAVE_PATH)

if __name__ == '__main__':
    __main__()