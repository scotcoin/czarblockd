# -*- coding: utf-8 -*-
VERSION = "1.5.0" #should keep up with the counterwallet version it works with (for now at least)

DB_VERSION = 22 #a db version increment will cause czarblockd to rebuild its database off of czarpartyd 

CAUGHT_UP = False #atomic state variable, set to True when czarpartyd AND czarblockd are caught up

UNIT = 100000000

SUBDIR_ASSET_IMAGES = "asset_img" #goes under the data dir and stores retrieved asset images
SUBDIR_FEED_IMAGES = "feed_img" #goes under the data dir and stores retrieved feed images

MARKET_PRICE_DERIVE_NUM_POINTS = 8 #number of last trades over which to derive the market price (via WVAP)

# FROM czarpartyd
# NOTE: These constants must match those in czarpartyd/lib/config.py
REGULAR_DUST_SIZE = 5430
MULTISIG_DUST_SIZE = 5430 * 2
ORDER_CZR_DUST_LIMIT_CUTOFF = MULTISIG_DUST_SIZE

mongo_db = None #will be set on server init

CZR = 'CZR'
XZR = 'XZR'

MAX_REORG_NUM_BLOCKS = 10 #max reorg we'd likely ever see

ARMORY_UTXSVR_PORT_MAINNET = 6590
ARMORY_UTXSVR_PORT_TESTNET = 6591

QUOTE_ASSETS = ['CZR', 'XCZR', 'XZR'] # define the priority for quote asset
MARKET_LIST_QUOTE_ASSETS = ['XZR', 'XCZR', 'CZR'] # define the order in the market list