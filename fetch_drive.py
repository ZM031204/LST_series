from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

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

def download_and_clean(drive,folder_name, save_path):
    # 创建保存目录
    os.makedirs(save_path, exist_ok=True)
    
    folder_id = get_folder_id_by_name(drive,folder_name)
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