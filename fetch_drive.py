from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import time
from datetime import datetime
def check_task_status(task, gap = 20):
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
        time.sleep(gap)  # 10秒检查一次


def create_folder(drive,parent_folder_id,folder_name):
    folder_metadata = {
        'title': folder_name,
        'parents': [{'id': parent_folder_id}],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    try:
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()

        print(f"已创建文件夹: {folder_name}")
        print(f"文件夹ID: {folder['id']}")
        return folder['id']
    except Exception as e:
        print(f"文件夹创建失败: {str(e)}")
        return None
    
def get_folder_id_by_name(drive, folder_name, parent_id='root'):
    """通过文件夹名称获取ID"""
    query = f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    if file_list:
        return file_list[0]['id']
    return None

def download_and_clean(drive,folder_id, save_path):
    # 创建保存目录
    os.makedirs(save_path, exist_ok=True)
    
    # 查询文件夹内容（排除子文件夹）
    file_list = drive.ListFile({
        'q': f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'",
        'maxResults': 1000
    }).GetList()
    
    # 下载并删除文件
    for index, file_obj in enumerate(file_list):
        try:
            # 下载文件
            filename = os.path.join(save_path, file_obj['title'])
            print(f"({index+1}/{len(file_list)}) 正在下载 {file_obj['title']}")
            file_obj.GetContentFile(filename)
            
            # 删除云端文件
            file_obj.Delete()
            print(f"√ 已成功删除云端文件")
            
        except Exception as e:
            print(f"× 文件处理失败: {str(e)}")
            continue