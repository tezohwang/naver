# 앱에 쓰이는 상수 선언

DATABASE = {
    'diana': {
        'db_uri': 'mongodb://wizpace:wizpace0@52.78.216.222:27017/diana',
        'db_name': 'diana'
    },
    'autobidding':{
        'db_uri': 'mongodb://autobidding:autobid0@13.125.87.196:27017/autobidding',
        'db_name': 'autobidding'
    }
}
FB_APP = {
    'app_id': '1633923886632047',
    'app_secret': '4ec9658c91944be5fe71e0c20fcb3786'
}

FETCH = {
    # yesterday
    'from_days':1,
    # minimum impression limit
    'min_imp_limit':10
}

MAIL = {
    'login_id': 'sb63w1@gmail.com',
    'login_pw': 'xowhghkd1!A',
    'from': 'sb63w1@gmail.com',
    'recipients': [
        'support@wizpace.com',
        'tony.hwang@wizpace.com',
        # 'danbee@wizpace.com',
        # 'jusung@wizpace.com'
    ]
}
