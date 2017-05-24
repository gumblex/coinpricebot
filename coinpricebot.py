#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import time
import json
import logging
import itertools
import configparser
import concurrent.futures

import requests

logging.basicConfig(stream=sys.stderr, format='%(asctime)s [%(name)s:%(levelname)s] %(message)s', level=logging.DEBUG if sys.argv[-1] == '-v' else logging.INFO)

HSession = requests.Session()

re_mdlink = re.compile(r'\[(.+?)\]\(.+?\)')

class BotAPIFailed(Exception):
    def __init__(self, ret):
        self.ret = ret
        self.description = ret['description']
        self.error_code = ret['error_code']
        self.parameters = ret.get('parameters')

    def __repr__(self):
        return 'BotAPIFailed(%r)' % self.ret

class TelegramBotClient:
    def __init__(self, apitoken, username=None):
        self.token = apitoken
        if username:
            self.username = username
        else:
            self.username = self.bot_api('getMe')['username']
        self.offset = None
        self.run = True

    def bot_api(self, method, **params):
        for att in range(3):
            try:
                req = HSession.post(('https://api.telegram.org/bot%s/' %
                                    self.token) + method, data=params, timeout=45)
                retjson = req.content
                ret = json.loads(retjson.decode('utf-8'))
                break
            except Exception as ex:
                if att < 1:
                    time.sleep((att + 1) * 2)
                else:
                    raise ex
        if not ret['ok']:
            raise BotAPIFailed(ret)
        return ret['result']

    def parse_cmd(self, text: str):
        t = text.strip().replace('\xa0', ' ').split(' ', 1)
        if not t:
            return None, None
        cmd = t[0].rsplit('@', 1)
        if len(cmd[0]) < 2 or cmd[0][0] != '/':
            return None, None
        if len(cmd) > 1 and cmd[-1] != self.username:
            return None, None
        expr = t[1] if len(t) > 1 else ''
        return cmd[0][1:], expr

    def serve(self, **kwargs):
        '''
        **kwargs is a map for callbacks. For example: {'message': process_msg}
        '''
        while self.run:
            try:
                updates = self.bot_api('getUpdates', offset=self.offset, timeout=30)
            except BotAPIFailed as ex:
                if ex.parameters and 'retry_after' in ex.parameters:
                    time.sleep(ex.parameters['retry_after'])
            except Exception:
                logging.exception('Get updates failed.')
                continue
            if not updates:
                continue
            self.offset = updates[-1]["update_id"] + 1
            for upd in updates:
                for k, v in upd.items():
                    if k == 'update_id':
                        continue
                    elif kwargs.get(k):
                        kwargs[k](self, v)
            time.sleep(.2)

    def __getattr__(self, name):
        return lambda **kwargs: self.bot_api(name, **kwargs)

class CoinPriceAPI:

    POLONIEX_MKTS = frozenset((
        'AMP_BTC', 'ARDR_BTC', 'BBR_BTC', 'BCN_BTC', 'BCY_BTC', 'BELA_BTC',
        'BITS_BTC', 'BLK_BTC', 'BTCD_BTC', 'BTM_BTC', 'BTS_BTC', 'BURST_BTC',
        'BTC_C2', 'CLAM_BTC', 'CURE_BTC', 'DASH_BTC', 'DCR_BTC', 'DGB_BTC',
        'DOGE_BTC', 'BTC_EMC2', 'ETC_BTC', 'ETH_BTC', 'EXP_BTC', 'FCT_BTC',
        'FLDC_BTC', 'FLO_BTC', 'GAME_BTC', 'GNT_BTC', 'GRC_BTC', 'HUC_BTC',
        'HZ_BTC', 'IOC_BTC', 'LBC_BTC', 'LSK_BTC', 'LTC_BTC', 'MAID_BTC',
        'MYR_BTC', 'NAUT_BTC', 'NAV_BTC', 'NEOS_BTC', 'NMC_BTC', 'NOBL_BTC',
        'NOTE_BTC', 'NSR_BTC', 'NXC_BTC', 'NXT_BTC', 'OMNI_BTC', 'PASC_BTC',
        'PINK_BTC', 'POT_BTC', 'PPC_BTC', 'QBK_BTC', 'QORA_BTC', 'QTL_BTC',
        'RADS_BTC', 'RBY_BTC', 'REP_BTC', 'RIC_BTC', 'SBD_BTC', 'SC_BTC',
        'SDC_BTC', 'SJCX_BTC', 'STEEM_BTC', 'STR_BTC', 'STRAT_BTC', 'SYS_BTC',
        'UNITY_BTC', 'VIA_BTC', 'VOX_BTC', 'VRC_BTC', 'VTC_BTC', 'XBC_BTC',
        'XCP_BTC', 'XEM_BTC', 'XMG_BTC', 'XMR_BTC', 'XPM_BTC', 'XRP_BTC',
        'XVC_BTC', 'ZEC_BTC', 'ETC_ETH', 'GNT_ETH', 'LSK_ETH', 'REP_ETH',
        'STEEM_ETH', 'ZEC_ETH', 'BTC_USDT', 'DASH_USDT', 'ETC_USDT', 'ETH_USDT',
        'LTC_USDT', 'NXT_USDT', 'REP_USDT', 'STR_USDT', 'XMR_USDT', 'XRP_USDT',
        'ZEC_USDT', 'BBR_XMR', 'BCN_XMR', 'BLK_XMR', 'BTCD_XMR', 'DASH_XMR',
        'LTC_XMR', 'MAID_XMR', 'NXT_XMR', 'QORA_XMR', 'ZEC_XMR',
    ))

    COINBASE_MKTS = frozenset('BTC_' + k for k in (
        'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN',
        'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL',
        'BSD', 'BTC', 'BTN', 'BWP', 'BYN', 'BYR', 'BZD', 'CAD', 'CDF', 'CHF',
        # , 'CNY'
        'CLF', 'CLP', 'COP', 'CRC', 'CUC', 'CVE', 'CZK', 'DJF', 'DKK',
        'DOP', 'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'ETH', 'EUR', 'FJD', 'FKP',
        'GBP', 'GEL', 'GGP', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD',
        'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'IMP', 'INR', 'IQD', 'ISK',
        'JEP', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KRW', 'KWD',
        'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL', 'LYD',
        'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MTL', 'MUR',
        'MVR', 'MWK', 'MXN', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR',
        'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR',
        'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SEK', 'SGD', 'SHP',
        'SLL', 'SOS', 'SRD', 'SSP', 'STD', 'SVC', 'SZL', 'THB', 'TJS', 'TMT',
        'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU',
        'UZS', 'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XCD', 'XDR',
        'XOF', 'XPD', 'XPF', 'XPT', 'YER', 'ZAR', 'ZMK', 'ZMW', 'ZWL'
    ))

    BTCCHINA_MKTS = frozenset(('BTC_CNY', 'LTC_CNY'))

    YAHOO_MKTS = {'USD_CNY': 'USDCNY', 'JPY_CNY': 'JPYCNY'}

    match = {x.replace('_', ''): x for x in itertools.chain(
             POLONIEX_MKTS, COINBASE_MKTS, BTCCHINA_MKTS, YAHOO_MKTS)}

    def __init__(self, ttl=60):
        self.ttl = ttl
        self._last_update = {}
        self._price = {}
        self.source = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def update_poloniex(self):
        req = HSession.get('https://poloniex.com/public?command=returnTicker', timeout=30)
        req.raise_for_status()
        update = time.time()
        ret = req.json()
        for k, v in ret.items():
            pair = '_'.join(reversed(k.split('_')))
            if pair not in self.POLONIEX_MKTS:
                continue
            self._last_update[pair] = update
            self._price[pair] = v['last']
            self.source[pair] = 'https://poloniex.com/exchange#' + k.lower()

    def update_coinbase(self):
        req = HSession.get('https://api.coinbase.com/v2/exchange-rates?currency=BTC', timeout=30)
        req.raise_for_status()
        update = time.time()
        ret = req.json()['data']
        currency = ret['currency']
        for k, v in ret['rates'].items():
            pair = '%s_%s' % (currency, k)
            if pair not in self.COINBASE_MKTS:
                continue
            self._last_update[pair] = update
            self._price[pair] = v
            self.source[pair] = 'https://www.coinbase.com/charts'

    def update_btcchina(self):
        req = HSession.get('https://data.btcchina.com/data/ticker?market=all', timeout=30)
        req.raise_for_status()
        ret = req.json()
        for k, pair in (
            ('ticker_btccny', 'BTC_CNY'), ('ticker_ltccny', 'LTC_CNY')):
            v = ret[k]
            self._last_update[pair] = v['date']
            self._price[pair] = v['last']
            self.source[pair] = 'https://spot.btcc.com/'

    def update_yahoo(self, key, symbol):
        req = HSession.get('http://download.finance.yahoo.com/d/quotes.csv?e=.csv&f=l1&s=%s=X' % symbol, timeout=30)
        req.raise_for_status()
        ret = req.text.strip()
        self._last_update[key] = time.time()
        self._price[key] = ret
        self.source[key] = 'http://finance.yahoo.com/quote/%s=X' % symbol

    def __getitem__(self, key):
        if time.time() - self._last_update.get(key, 0) < self.ttl:
            return self._price[key]
        elif key in self.YAHOO_MKTS:
            logging.debug('yahoo: ' + key)
            self.update_yahoo(key, self.YAHOO_MKTS[key])
        elif key in self.BTCCHINA_MKTS:
            logging.debug('btcc: ' + key)
            self.update_btcchina()
        elif key in self.COINBASE_MKTS:
            logging.debug('coinbase: ' + key)
            self.update_coinbase()
        else:
            logging.debug('poloniex: ' + key)
            self.update_poloniex()
        return self._price[key]

    def getmany(self, keys):
        return dict(zip(keys, self.executor.map(self.__getitem__, keys)))

text_template = '''[BTCUSD](https://www.coinbase.com/charts)=%s [BTCCNY](https://spot.btcc.com/)=%s
[LTCBTC](https://poloniex.com/exchange#btc_ltc)=%s [LTCCNY](https://spot.btcc.com/)=%s
[USDCNY](http://finance.yahoo.com/quote/CNY=X)=%s [JPYCNY](http://finance.yahoo.com/quote/JPYCNY=X)=%s
[ZECBTC](https://poloniex.com/exchange#btc_zec)=%s ZECUSD=%.4f
[XMRBTC](https://poloniex.com/exchange#btc_xmr)=%s XMRUSD=%.4f'''

price_api = CoinPriceAPI(60)

def message_handler(cli, msg):
    msgtext = msg.get('text', '')
    cmd, expr = cli.parse_cmd(msgtext)
    if not cmd:
        return
    elif cmd == 'query' and not expr:
        try:
            price = price_api.getmany((
                'BTC_USD', 'BTC_CNY', 'LTC_BTC', 'LTC_CNY',
                'USD_CNY', 'JPY_CNY', 'ZEC_BTC', 'XMR_BTC'))
            text = text_template % (
                price['BTC_USD'], price['BTC_CNY'],
                price['LTC_BTC'], price['LTC_CNY'],
                price['USD_CNY'], price['JPY_CNY'],
                price['ZEC_BTC'],
                float(price['ZEC_BTC']) * float(price['BTC_USD']),
                price['XMR_BTC'],
                float(price['XMR_BTC']) * float(price['BTC_USD'])
            )
        except Exception:
            logging.exception('Failed command: ' + msgtext)
            text = 'Failed to fetch data. Please try again later.'
        cli.sendMessage(chat_id=msg['chat']['id'], text=text,
                        parse_mode='Markdown', disable_web_page_preview=True)
        logging.info('query: ' + re_mdlink.sub(r'\1', text.replace('\n', ' ')))
    elif cmd == 'query':
        try:
            key = expr.strip().upper()
            if '_' not in key:
                key = price_api.match[key]
            price = price_api[key]
            cli.sendMessage(chat_id=msg['chat']['id'], text="[%s](%s)=%s" % (
                expr, price_api.source[key], price
            ), parse_mode='Markdown', disable_web_page_preview=True)
        except KeyError:
            cli.sendMessage(chat_id=msg['chat']['id'], text="We don't have data source for %s." % expr)
        except Exception:
            logging.exception('Failed command: ' + msgtext)
            cli.sendMessage(chat_id=msg['chat']['id'], text="Failed to fetch data. Please try again later.")
    elif cmd == 'start':
        return

def load_config(filename):
    cp = configparser.ConfigParser()
    cp.read(filename)
    return cp

def main():
    config = load_config('config.ini')
    botcli = TelegramBotClient(
        config['Bot']['apitoken'], config['Bot'].get('username'))
    logging.info('Satellite launched.')
    botcli.serve(message=message_handler)

if __name__ == '__main__':
    main()
