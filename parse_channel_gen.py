import requests
import xmltodict
import pandas as pd
import VitalityBooster as vb
import DataBase as db


channels = []
channel_id = []
channel_ip = []
genre_dict = {}
genre_channel_list = []

insert = '''insert into iptv_cache.iptv_channels_nsk 
            (vlc_id, channel_ip, channel_name, category) values {insert_here}'''
datalake_truncate = '''truncate iptv_cache.iptv_channels_nsk'''
request = requests.get('http://tvguide.sibset.ru/playlist/eltex/hls', stream=True)
dict_data = xmltodict.parse(request.content)
for i in dict_data['playlist']['trackList']['track']:
    channels.append(i['title'])
    channel_id.append(i['extension']['vlc:id'])
    channel_ip.append(i['location'].split('/')[3])
for i in dict_data['playlist']['extension']['vlc:node']:
    if i['@title'] != 'Все':
        for name in i['vlc:item']:
            genre_dict[name['@tid']] = i['@title']
for i in channel_id:
    if i in genre_dict:
        genre_channel_list.append(genre_dict[i])
data = {'vlc_id': channel_id,
        'channel_ip': channel_ip,
        'channel_name': channels,
        'category': genre_channel_list}
df = pd.DataFrame(data)
datalake = vb.MessengerSQL(db.PostgreSQL_Datalake())
datalake.connect()
datalake.send_command_no_data(datalake_truncate)
datalake.execute_method(insert, df)
