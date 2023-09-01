import os
import sys
import path
import logging



basePath = path.func()

logger = logging.getLogger('my_logger')
logging.basicConfig(filename=basePath+'logs/autogeneratePythonLogs.log', filemode='w', level=logging.DEBUG)

try:
    os.system('python3 ' + 'StoreSnaps.py' "'"+ basePath+ "'")
    os.system('python3 ' + 'Whatsapp.py' "'"+ basePath+ "'")    
except:
    print('Failed')