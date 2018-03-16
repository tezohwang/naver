from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from celery import shared_task

from .database import *
from .constant import *
from .api.managedCustomerLink import *
from .api.campaign import *
from .api.adgroup import *
from .api.keyword import *
from .api.stat import *

import asyncio, datetime, time, json

# Route Views
def index(request):
    context = {}
    return HttpResponse("index page")

@csrf_exempt
def first_login(request):
    if request.method == "POST":
        # print(request.body)
        # print(type(json.loads(request.body)))
        req = json.loads(request.body.decode('utf-8'))
        fetch_past_all_process.delay(str(req['network_id']))
        return HttpResponse("success")
    return HttpResponse("error")

# Controll Views
def fetch_accounts():
    db = connect_db('diana')
    accounts = []
    users = list(db['users'].find({'type': 'naver'}))
    if not users:
        return [], db
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
                'x_secrete': user['x_secrete'],
            })
    update_accounts(accounts)
    print("fetch_accounts done - {}".format(datetime.datetime.now()))
    return accounts, db

def first_fetch_accounts(network_id):
    db = connect_db('diana')
    accounts = []
    user = dict(db['users'].find_one({
        'type':'naver',
        'network_id':network_id,
    }))
    if not 'name' in user:
        user['name'] = 'main'
    accounts.append(user)
    params = {'type':'MYCLIENTS'}
    clients = get_clients_list(user, params)
    for client in clients:
        accounts.append({
            'user_id': user['user_id'],
            'type': 'naver',
            'network_id': str(client['clientCustomerId']),
            'name': client['clientLoginId'],
            'x_api_key': user['x_api_key'],
            'x_secrete': user['x_secrete'],
        })
    update_accounts(accounts)
    print("fetch_accounts done - {}".format(datetime.datetime.now()))
    return accounts, db

def update_accounts(accounts):
    db = connect_db('diana')
    nvtest_accounts = db['nvtest_accounts']
    for account in accounts:
        nvtest_accounts.update_one(
            {'network_id': account['network_id']},
            {
                '$set':{
                    'user_id':account['user_id'],
                    'type': 'naver',
                    'name': account['name'],
                    'x_api_key': account['x_api_key'],
                    'x_secrete': account['x_secrete'],
                },
            },
            upsert=True,
        )
        print("update_accounts done - {}".format(datetime.datetime.now()))
    return accounts

def fetch_campaigns(account, db):
    campaigns = get_campaigns_list(account)
    if not campaigns:
        return [], db
    # print(campaigns)
    ids = [campaign['nccCampaignId'] for campaign in campaigns]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['campaign'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        # print(stats)
        print("get_by_ids(account) done - {}".format(datetime.datetime.now()))
        if 'data' in stats:
            for data in stats['data']:
                data['user_id'] = account['user_id']
                data['network_id'] = account['network_id']
                data['dateStart'] = datetime.datetime.now()
                data['dateEnd'] = datetime.datetime.now()
                data['date'] = datetime.datetime.now().strftime('%Y%m%d')
            stats['data'] = combine_campaigns(campaigns, stats['data'])
            update_campaigns(stats['data'], db)
            return stats['data'], db
    return [], db

def first_fetch_campaigns(account, db):
    campaigns = get_campaigns_list(account)
    # print(campaigns)
    if not campaigns:
        return [], db
    for campaign in campaigns:
        # print(campaign)
        params = {
            'id':campaign['nccCampaignId'],
            'fields': FIELDS['campaign'],
            'timeRange': get_time_range(60),
            'timeIncrement': '1',
        }
        stats = get_by_ids(account, params)
        print("get_by_ids(account) done - {}".format(datetime.datetime.now()))
        # print(stats)
        if 'data' in stats:
            if not stats['data']:
                continue
            for data in stats['data']:
                data['user_id'] = account['user_id']
                data['network_id'] = account['network_id']
                data['dateStart'] = datetime.datetime.strptime(data['dateStart'], "%Y-%m-%d")
                data['dateEnd'] = datetime.datetime.strptime(data['dateEnd'], "%Y-%m-%d")
                data['date'] = data['dateEnd'].strftime('%Y%m%d')
                data['id'] = campaign['nccCampaignId']
                data['nccCampaignId'] = campaign['nccCampaignId']
                data['customerId'] = campaign['customerId']
                data['name'] = campaign['name']
                data['userLock'] = campaign['userLock']
                data['regTm'] = campaign['regTm']
                data['editTm'] = campaign['editTm']
                data['dailyBudget'] = campaign['dailyBudget']
                data['useDailyBudget'] = campaign['useDailyBudget']
                data['status'] = campaign['status']
                data['statusReason'] = campaign['statusReason']
                data['expectCost'] = campaign['expectCost']
                data['migType'] = campaign['migType']
            # update_campaigns(stats['data'], db)
            insert_campaigns(stats['data'], db)
    return campaigns, db

def combine_campaigns(campaigns, data):
    for _data in data:
        for campaign in campaigns:
            if campaign['nccCampaignId'] == _data['id']:
                _data.update(campaign)
    # print(data)
    print("combine_campaigns done - {}".format(datetime.datetime.now()))
    return data

def insert_campaigns(campaigns, db):
    nvtest_campaigns = db['nvtest_campaigns']
    nvtest_campaigns.insert_many(campaigns)
    # print("insert_campaigns done")
    return db

def update_campaigns(campaigns, db):
    # db = connect_db('diana')
    nvtest_campaigns = db['nvtest_campaigns']
    for campaign in campaigns:
        nvtest_campaigns.update_one(
            {
                'id':campaign['id'],
                'date':campaign['date'],
            },
            {
                '$set':{
                    'user_id':campaign['user_id'],
                    'network_id':campaign['network_id'],
                    'dateStart':campaign['dateStart'],
                    'dateEnd':campaign['dateEnd'],
                    'impCnt':campaign['impCnt'],
                    'clkCnt':campaign['clkCnt'],
                    'ctr':campaign['ctr'],
                    'cpc':campaign['cpc'],
                    'ccnt':campaign['ccnt'],
                    'salesAmt':campaign['salesAmt'],
                    'nccCampaignId':campaign['nccCampaignId'],
                    'customerId':campaign['customerId'],
                    'name':campaign['name'],
                    'userLock':campaign['userLock'],
                    'regTm':campaign['regTm'],
                    'editTm':campaign['editTm'],
                    'dailyBudget':campaign['dailyBudget'],
                    'useDailyBudget':campaign['useDailyBudget'],
                    'status':campaign['status'],
                    'statusReason':campaign['statusReason'],
                    'expectCost':campaign['expectCost'],
                    'migType':campaign['migType'],
                }
            },
            upsert=True,
        )
    print("update_campaigns done - {}".format(datetime.datetime.now()))
    return campaigns, db

def fetch_adgroups(account, campaign, db):
    params = {'nccCampaignId':campaign['id']}
    adgroups = get_adgroups_list(account, params)
    if not adgroups:
        return [], db
    # print(adgroups)
    ids = [adgroup['nccAdgroupId'] for adgroup in adgroups]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['adgroup'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        # print(stats)
        if 'data' in stats:
            for data in stats['data']:
                data['network_id'] = account['network_id']
                data['campaign_id'] = campaign['id']
                data['dateStart'] = datetime.datetime.now()
                data['dateEnd'] = datetime.datetime.now()
                data['date'] = datetime.datetime.now().strftime('%Y%m%d')
            stats['data'] = combine_adgroups(adgroups, stats['data'])
            update_adgroups(stats['data'], db)
            print("fetch_adgroups done - {}".format(datetime.datetime.now()))
            return stats['data'], db
    return [], db

def first_fetch_adgroups(account, campaign, db):
    params = {'nccCampaignId':campaign['nccCampaignId']}
    adgroups = get_adgroups_list(account, params)
    # print(adgroups)
    if not adgroups:
        return [], db
    for adgroup in adgroups:
        # print(adgroup)
        params = {
            'id':adgroup['nccAdgroupId'],
            'fields': FIELDS['adgroup'],
            'timeRange': get_time_range(60),
            'timeIncrement': '1',
        }
        stats = get_by_ids(account, params)
        print("get_by_ids(account) done - {}".format(datetime.datetime.now()))
        # print(stats)
        if 'data' in stats:
            if not stats['data']:
                continue
            for data in stats['data']:
                data['user_id'] = account['user_id']
                data['network_id'] = account['network_id']
                data['dateStart'] = datetime.datetime.strptime(data['dateStart'], "%Y-%m-%d")
                data['dateEnd'] = datetime.datetime.strptime(data['dateEnd'], "%Y-%m-%d")
                data['date'] = data['dateEnd'].strftime('%Y%m%d')
                data['id'] = adgroup['nccAdgroupId']
                data['campaign_id'] = campaign['nccCampaignId']
                data['nccCampaignId'] = adgroup['nccCampaignId']
                data['nccAdgroupId'] = adgroup['nccAdgroupId']
                data['customerId'] = adgroup['customerId']
                data['name'] = adgroup['name']
                data['mobileChannelId'] = adgroup['mobileChannelId']
                data['pcChannelId'] = adgroup['pcChannelId']
                data['bidAmt'] = adgroup['bidAmt']
                data['useKeywordPlus'] = adgroup['useKeywordPlus']
                data['keywordPlusWeight'] = adgroup['keywordPlusWeight']
                data['contentsNetworkBidAmt'] = adgroup['contentsNetworkBidAmt']
                data['useCntsNetworkBidAmt'] = adgroup['useCntsNetworkBidAmt']
                data['mobileNetworkBidWeight'] = adgroup['mobileNetworkBidWeight']
                data['pcNetworkBidWeight'] = adgroup['pcNetworkBidWeight']
                data['userLock'] = adgroup['userLock']
                data['regTm'] = adgroup['regTm']
                data['editTm'] = adgroup['editTm']
                data['dailyBudget'] = adgroup['dailyBudget']
                data['useDailyBudget'] = adgroup['useDailyBudget']
                data['budgetLock'] = adgroup['budgetLock']
                data['status'] = adgroup['status']
                data['statusReason'] = adgroup['statusReason']
                data['targetSummary'] = adgroup['targetSummary']
                data['pcChannelKey'] = adgroup['pcChannelKey']
                data['expectCost'] = adgroup['expectCost']
                data['migType'] = adgroup['migType']
            # update_adgroups(stats['data'], db)
            insert_adgroups(stats['data'], db)
            print("first_fetch_adgroups done - {}".format(datetime.datetime.now()))
    return adgroups, db

def combine_adgroups(adgroups, data):
    for _data in data:
        for adgroup in adgroups:
            if adgroup['nccAdgroupId'] == _data['id']:
                _data.update(adgroup)
    # print(data)
    print("combine_adgroups done - {}".format(datetime.datetime.now()))
    return data

def insert_adgroups(adgroups, db):
    nvtest_adgroups = db['nvtest_adgroups']
    nvtest_adgroups.insert_many(adgroups)
    # print("insert_adgroups done")
    return db

def update_adgroups(adgroups, db):
    # db = connect_db('diana')
    nvtest_adgroups = db['nvtest_adgroups']
    for adgroup in adgroups:
        nvtest_adgroups.update_one(
            {
                'id':adgroup['id'],
                'date':adgroup['date']
            },
            {
                '$set':{
                    'network_id':adgroup['network_id'],
                    'campaign_id':adgroup['campaign_id'],
                    'dateStart':adgroup['dateStart'],
                    'dateEnd':adgroup['dateEnd'],
                    'impCnt':adgroup['impCnt'],
                    'clkCnt':adgroup['clkCnt'],
                    'ctr':adgroup['ctr'],
                    'cpc':adgroup['cpc'],
                    'ccnt':adgroup['ccnt'],
                    'salesAmt':adgroup['salesAmt'],
                    'nccAdgroupId':adgroup['nccAdgroupId'],
                    'customerId':adgroup['customerId'],
                    'nccCampaignId':adgroup['nccCampaignId'],
                    'mobileChannelId':adgroup['mobileChannelId'],
                    'pcChannelId':adgroup['pcChannelId'],
                    'bidAmt':adgroup['bidAmt'],
                    'name':adgroup['name'],
                    'userLock':adgroup['userLock'],
                    'useDailyBudget':adgroup['useDailyBudget'],
                    'useKeywordPlus':adgroup['useKeywordPlus'],
                    'keywordPlusWeight':adgroup['keywordPlusWeight'],
                    'contentsNetworkBidAmt':adgroup['contentsNetworkBidAmt'],
                    'useCntsNetworkBidAmt':adgroup['useCntsNetworkBidAmt'],
                    'mobileNetworkBidWeight':adgroup['mobileNetworkBidWeight'],
                    'pcNetworkBidWeight':adgroup['pcNetworkBidWeight'],
                    'dailyBudget':adgroup['dailyBudget'],
                    'budgetLock':adgroup['budgetLock'],
                    'regTm':adgroup['regTm'],
                    'editTm':adgroup['editTm'],
                    'targetSummary':adgroup['targetSummary'],
                    'pcChannelKey':adgroup['pcChannelKey'],
                    'status':adgroup['status'],
                    'statusReason':adgroup['statusReason'],
                    'expectCost':adgroup['expectCost'],
                    'migType':adgroup['migType'],
                }
            },
            upsert=True,
        )
    print("update_adgroups done - {}".format(datetime.datetime.now()))
    return adgroups, db

def fetch_keywords(account, adgroup):
    params = {'nccAdgroupId':adgroup['id']}
    keywords = get_keywords_list(account, params)
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
    start_time = time.time()
    #------------------------

    accounts, db = fetch_accounts()
    for account in accounts:
        campaigns, db = fetch_campaigns(account, db)
        if campaigns:
            for campaign in campaigns:
                adgroups, db = fetch_adgroups(account, campaign, db)
                if adgroups:
                    for adgroup in adgroups:
                        keywords = async_fetch_keywords(account, campaign, adgroup, db)
    
    #------------------------
    print("start_time", start_time)
    print("--- %s seconds ---" %(time.time() - start_time))
    return print("fetch_process done!")

def async_fetch_keywords(account, campaign, adgroup, db):
    params = {'nccAdgroupId':adgroup['id']}
    keywords = get_keywords_list(account, params)
    if not keywords:
        return []
    # print(keywords)
    ids = [keyword['nccKeywordId'] for keyword in keywords]
    ids_set = [ids[i:i+100] for i in range(0, len(ids), 100)]
    params = {
        'fields': FIELDS['keyword'],
        'datePreset': DATEPRESET,
    }
    stats = [async_get_by_ids(account, campaign, adgroup, keywords, params, ids, db) for ids in ids_set]
    # print(stats)
    loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.wait(stats))
    print("async_fetch_keywords done - {}".format(datetime.datetime.now()))
    return stats[0]

def async_first_fetch_keywords(account, campaign, adgroup, db):
    params = {'nccAdgroupId':adgroup['nccAdgroupId']}
    keywords = get_keywords_list(account, params)
    if not keywords:
        return [], db
    # ids = [keyword['nccKeywordId'] for keyword in keywords]
    # print(ids)
    params = {
        'fields': FIELDS['keyword'],
        'timeRange': get_time_range(60),
        'timeIncrement': '1',
    }
    stats = [async_get_by_id(account, campaign, adgroup, keyword, params, db) for keyword in keywords]
    print(stats)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(stats))
    # print("async_first_fetch_keywords done - {}".format(datetime.datetime.now()))
    return stats[0], db

async def async_get_by_ids(account, campaign, adgroup, keywords, params, ids, db):
    params['ids'] = ids
    time_now = str(int(round(time.time() * 1000)))
    http_method = "GET"
    request_uri = "/stats"
    headers = {
        'X-Timestamp': time_now,
        'X-API-KEY': account['x_api_key'],
        'X-Customer': account['network_id'],
        'X-Signature': gen_signature(time_now, http_method, request_uri, account['x_secrete']),
    }
    response = requests.get('https://api.naver.com' + request_uri, headers=headers, params=params)
    json_res_data = json.loads(response.text)
    if 'data' in json_res_data:
        for data in json_res_data['data']:
            data['network_id'] = account['network_id']
            data['campaign_id'] = campaign['id']
            data['adgroup_id'] = adgroup['id']
            data['dateStart'] = datetime.datetime.now()
            data['dateEnd'] = datetime.datetime.now()
            data['date'] = datetime.datetime.now().strftime('%Y%m%d')
        json_res_data['data'] = combine_keywords(keywords, json_res_data['data'])
        update_keywords(json_res_data['data'], db)
    print("async_get_by_ids done - {}".format(datetime.datetime.now()))
    return json_res_data['data']

async def async_get_by_id(account, campaign, adgroup, keyword, params, db):
    params['id'] = keyword['nccKeywordId']
    print(params['id'])
    time_now = str(int(round(time.time() * 1000)))
    http_method = "GET"
    request_uri = "/stats"
    headers = {
        'X-Timestamp': time_now,
        'X-API-KEY': account['x_api_key'],
        'X-Customer': account['network_id'],
        'X-Signature': gen_signature(time_now, http_method, request_uri, account['x_secrete']),
    }
    response = requests.get('https://api.naver.com' + request_uri, headers=headers, params=params)
    json_res_data = json.loads(response.text)
    # print(keyword)
    # print(json_res_data)
    if 'data' in json_res_data:
        for data in json_res_data['data']:
            data['id'] = keyword['nccKeywordId']
            data['network_id'] = account['network_id']
            data['campaign_id'] = campaign['nccCampaignId']
            data['adgroup_id'] = adgroup['nccAdgroupId']
            data['dateStart'] = datetime.datetime.strptime(data['dateStart'], "%Y-%m-%d")
            data['dateEnd'] = datetime.datetime.strptime(data['dateEnd'], "%Y-%m-%d")
            data['date'] = data['dateEnd'].strftime('%Y%m%d')
            data['nccKeywordId'] = keyword['nccKeywordId']
            data['keyword'] = keyword['keyword']
            data['customerId'] = keyword['customerId']
            data['nccAdgroupId'] = keyword['nccAdgroupId']
            data['nccCampaignId'] = keyword['nccCampaignId']
            data['userLock'] = keyword['userLock']
            data['inspectStatus'] = keyword['inspectStatus']
            data['bidAmt'] = keyword['bidAmt']
            data['useGroupBidAmt'] = keyword['useGroupBidAmt']
            data['regTm'] = keyword['regTm']
            data['editTm'] = keyword['editTm']
            data['status'] = keyword['status']
            data['statusReason'] = keyword['statusReason']
            data['nccQi'] = keyword['nccQi']
        # await update_keywords(json_res_data['data'], db)
        # update_keywords(json_res_data['data'], db)
        insert_keywords(json_res_data['data'], db)
    # print("async_get_by_id done - {}".format(datetime.datetime.now()))
    return json_res_data['data'], db

def combine_keywords(keywords, data):
    for _data in data:
        for keyword in keywords:
            if keyword['nccKeywordId'] == _data['id']:
                _data.update(keyword)
    # print(data)
    print("combine_keywords done - {}".format(datetime.datetime.now()))
    return data

def insert_keywords(keywords, db):
    nvtest_keywords = db['nvtest_keywords']
    nvtest_keywords.insert_many(keywords)
    # print("insert_many_keywords done")
    return db

def update_keywords(keywords, db):
    # db = connect_db('diana')
    nvtest_keywords = db['nvtest_keywords']
    # nvtest_keywords.insert_many(json_res_data['data'])
    for data in keywords:
        nvtest_keywords.update_one(
            {
                'id':data['id'],
                'date':data['date']
            },
            {
                '$set':{
                    'network_id':data['network_id'],
                    'campaign_id':data['campaign_id'],
                    'adgroup_id':data['adgroup_id'],
                    'dateStart':data['dateStart'],
                    'dateEnd':data['dateEnd'],
                    'date':data['date'],
                    'impCnt':data['impCnt'],
                    'clkCnt':data['clkCnt'],
                    'ctr':data['ctr'],
                    'cpc':data['cpc'],
                    'avgRnk':data['avgRnk'],
                    'ccnt':data['ccnt'],
                    'recentAvgRnk':data['recentAvgRnk'],
                    'recentAvgCpc':data['recentAvgCpc'],
                    'salesAmt':data['salesAmt'],
                    'nccKeywordId':data['nccKeywordId'],
                    'keyword':data['keyword'],
                    'customerId':data['customerId'],
                    'nccAdgroupId':data['nccAdgroupId'],
                    'nccCampaignId':data['nccCampaignId'],
                    'userLock':data['userLock'],
                    'inspectStatus':data['inspectStatus'],
                    'bidAmt':data['bidAmt'],
                    'useGroupBidAmt':data['useGroupBidAmt'],
                    'regTm':data['regTm'],
                    'editTm':data['editTm'],
                    'status':data['status'],
                    'statusReason':data['statusReason'],
                    'nccQi':data['nccQi'],
                }
            },
            upsert=True
        )
    # print("update_keywords done - {}".format(datetime.datetime.now()))
    return keywords, db

@shared_task
def fetch_past_all_process(network_id):
    start_time = time.time()
    #------------------------

    accounts, db = first_fetch_accounts(network_id)
    # print(accounts)
    for account in accounts:
        campaigns, db = first_fetch_campaigns(account, db)
        # print(campaigns)
        # continue
        if campaigns:
            for campaign in campaigns:
                adgroups, db = first_fetch_adgroups(account, campaign, db)
                # print(adgroups)
                # continue
                if adgroups:
                    for adgroup in adgroups:
                        keywords, db = async_first_fetch_keywords(account, campaign, adgroup, db)
    
    #------------------------
    print("start_time", start_time)
    print("--- %s seconds ---" %(time.time() - start_time))

    return print("fetch_past_all_process done!")

def get_time_range(days):
    today = datetime.datetime.now()
    today_date = today.strftime('%Y-%m-%d')
    lastday = today - datetime.timedelta(days=days)
    lastday_date = lastday.strftime('%Y-%m-%d')
    time_range = '{"since":"' + lastday_date + '",' + '"until":"' + today_date + '"}'
    return time_range
