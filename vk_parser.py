
import vk
import re
import sys
import os
from systemd import journal

# session = vk.Session()
vk_api = vk.API(access_token=os.environ['V_TOKEN'], v='5.131',
                lang='ru')

import gspread
# from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_file(os.getcwd()+'/config.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# open a google sheet
gs = gc.open_by_key(os.environ['G_KEY'])
# select a work sheet from its name
worksheet1 = gs.worksheet(os.environ['G_LIST'])

post = vk_api.newsfeed.search(count=200, q=os.environ['V_TAG'])
import numpy
import pandas
import time

sheet = gs.worksheet(os.environ['G_LIST'])
# print(post)
values = numpy.array(sheet.get_all_values())
keys = values[:, 0]

for p in post['items']:
    # print(p)

    username = ''
    link = ''
    if p['from_id'] > 0:
        time.sleep(0.5)
        user = vk_api.users.get(user_ids=p['from_id'])[0]
        username = user['first_name'] + ' ' + user['last_name']
        link = 'id' + str(p['from_id'])
    else:
        time.sleep(0.5)
        username = vk_api.groups.getById(group_ids=-p['from_id'])[0]['name']
        link = 'club' + str(-p['from_id'])
    sp = p['text'].splitlines()
    title = ''
    for s in sp:
        if (len(s) == 0):
            continue
        if (s[0] == '#'):
            continue
        title = s
        break
    df_dict = [{'comment_id': str(p['owner_id']) + '_' + str(p['id']),
                'comment_lnk': '=hyperlink("vk.com/wall' + str(p['owner_id']) + '_' + str(
                    p['id']) + '";"' + title + '")',
                'author_id': '=hyperlink("vk.com/' + link + '";"' + username + '")',
                # 'text': p['text'],
                'len': len(p['text'])}]
    if (str(df_dict[0]['comment_id']) in keys) or (len(p['text']) < 400):
        continue
    df = pandas.DataFrame(df_dict)
    df_values = df.values.tolist()
    gs.values_append(os.environ['G_LIST']+'!A1:D1000', {'valueInputOption': 'USER_ENTERED'}, {'values': df_values})