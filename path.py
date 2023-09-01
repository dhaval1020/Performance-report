import json


def func():
    try:
        data = json.load(open('env.json'))
        for key, value in data.items():
            if key=='path1':
                return value
        return '/home/varunesh.88/data-etl/'
    except:
        return '/home/varunesh.88/data-etl/'
    

def func1():
    try:
        data = json.load(open('env.json'))
        for key, value in data.items():
            if key=='path2':
                return value
        return '/home/wsuser/dump/'
    except:
        return '/home/wsuser/dump/'