from django.shortcuts import render

from .database import *
from .constant import *
from .api.managedCustomerLink import *
from .api.campaign import *
from .api.adgroup import *
from .api.keyword import *
from .api.stat import *

# Route Views
def index(request):
    context = {}
    return render(request, 'report/index.html', context)

# Controll Views
def fetch_accounts():
    db = connect_db('diana')
    users = list(db['users'].find({"type": "naver"}))
    accounts = []
    for user in users:
        if not 'name' in user:
            user['name'] = 'main'
        accounts.append(user)
        params = {'type': 'MYCLIENTS'}
        clients = get_clients_list(user, params)
        for client in clients:
            accounts.append({
                'user_id': user['user_id'],
                'type': 'naver',
                'network_id': str(client['clientCustomerId']),
                'name': client['clientLoginId'],
                'x_api_key': user['x_api_key'],
                'x_secrete': user['x_secrete']
            })
    return accounts

def fetch_campaigns(account):
    campaigns = get_campaigns_list(account)
    ids = [campaign['nccCampaignId'] for campaign in campaigns]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['campaign'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        if stats['data']:
            return stats['data']
    return []

def fetch_adgroups(account, campaign):
    params = {'nccCampaignId':campaign['id']}
    adgroups = get_adgroups_list(account, params)
    ids = [adgroup['nccAdgroupId'] for adgroup in adgroups]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['adgroup'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        if stats['data']:
            return stats['data']
    return []

def fetch_keywords(account, adgroup):
    params = {'nccAdgroupId':adgroup['id']}
    keywords = get_keywords_list(account, params)
    # print(keywords)
    ids = [keyword['nccKeywordId'] for keyword in keywords]
    ids_set = [ids[i:i+100] for i in range(0, len(ids), 100)]
    stats_set = []
    for ids in ids_set:
        if ids:
            params = {
                'ids': ids,
                'fields': FIELDS['keyword'],
                'datePreset': DATEPRESET,
            }
            stats = get_by_ids(account, params)
            if stats['data']:
                stats_set += stats['data']
    if stats_set:
        return stats_set
    return []

def fetch_process():
    accounts = fetch_accounts()
    for account in accounts:
        campaigns = fetch_campaigns(account)
        if campaigns:
            for campaign in campaigns:
                adgroups = fetch_adgroups(account, campaign)
                if adgroups:
                    for adgroup in adgroups:
                        keywords = fetch_keywords(account, adgroup)
                        print(len(keywords))
    return print("fetch_process done!")
