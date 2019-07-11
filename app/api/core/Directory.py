import os
import os.path

def getProjectDirectory():
    return os.getcwd()

def createDirectory(directory_name_str):
    if not os.path.exists(directory_name_str):
        os.makedirs(directory_name_str)

def createFile(file_name_str):
    if file_name_str is not None or file_name_str != '':
        file = open(str(file_name_str), "w")
        file.close()

def deleteFilesInDirectory(directory_name_str):
    if os.path.exists(directory_name_str):
        for root, dirs, files in os.walk(directory_name_str):
            for file in files:
                os.remove(os.path.join(root, file))