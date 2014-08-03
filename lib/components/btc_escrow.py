import os
import logging
import decimal
import base64
import json
from datetime import datetime

from lib import config, util, util_bitcoin, blockchain

def parse_order(db, message, cur_block_index, cur_block):
    #this all needs to be idempotent, due to the fact that we may be processing off of a reparse

    if not config.AUTO_BTC_ESCROW_ENABLE:
        return
    
    #determine if we have an auto btc escrow record for this order and merge in information
    record = db.autobtcescrow_transactions.find_one({'order_tx_hash': message['tx_hash']})
    if not record:
        return
    assert record['source_address'] is None or record['source_address'] == message['source']
    if record['source_address'] is None:
        record['source_address'] == message['source']
    
    if record['order_actual_amount'] is None:
        if message['give_asset'] == 'BTC':
            record['order_actual_amount'] = util_bitcoin.normalize_quantity(message['give_amount'])
        else:
            assert message['get_asset'] == 'BTC'
            record['order_actual_amount'] = util_bitcoin.normalize_quantity(message['get_amount'])
        if record['order_actual_amount'] != record['order_expected_amount']:
            logging.warn("AutoBTCEscrow: Expected amount from API create (%s) does not match expected amount from order (%s). Overwriting..." % (
                record['order_expected_amount'], record['order_actual_amount']))

    if record['order_expire_index'] is None:
         record['order_expire_index'] = message['expire_index']
    assert record['order_expire_index'] == message['expire_index']
    
    if record['status'] == 'new':
        #populate the BTC-tx related details (NOTE that the actual BTC transaction may still be unconfirmed)
        tx_info = blockchain.gettransaction(record['btc_deposit_tx_hash'])
        if not tx_info: #invalid tx_hash
            record['status'] = 'invalid'
        else:
            record['status'] = 'open'
            record['remaining_amount'] = tx_info['valueOut'] #normalized
    
    record.save()

def parse_order_match(db, message, cur_block_index, cur_block):
    #this all needs to be idempotent, due to the fact that we may be processing off of a reparse
    
    if not config.AUTO_BTC_ESCROW_ENABLE:
        return
    
    #is it for a BTC order that requires a BTCpay?
    if message['status'] != 'pending':
        return
    
    #determine if this is a match for one of the orders we should handle BTCpay for
    order_tx_hash = message['tx0_hash'] if message['forward_asset'] == 'BTC' else message['tx1_hash']
    record = db.autobtcescrow_orders.find_one({'order_tx_hash': order_tx_hash})
    if not record:
        return
    order_tx_amount= util_bitcoin.normalize_quantity(
        message['forward_quantity'] if record['forward_asset'] == 'BTC' else message['backward_quantity'])

    assert record['status'] == 'open'
    assert record['source_address'] == (message['tx0_address'] if record['order_tx_hash'] == message['tx0_hash'] else message['tx1_address'])
    assert record['remaining_amount'] > 0
    assert record['remaining_amount'] <= tx_info['valueOut']
    
    tx_info = blockchain.gettransaction(record['btc_deposit_tx_hash'])
    assert tx_info
    assert tx_info['confirmations'] != 0 #the BTC deposit TX must have at least 1 confirm to pay out on it
    
    #set up to make the btcpay in N blocks
    pay_destination = message['tx1_address'] if record['order_tx_hash'] == message['tx0_hash'] else message['tx0_address']
    db.autobtcescrow_pending_payments.insert({
        'target_block_index': cur_block_index + config.AUTOBTCESCROW_NUM_BLOCKS_FOR_BTCPAY,
        'autobtcescrow_order_id': record.id,
        'order_match_id': message['tx0_hash'] + message['tx1_hash'],
        'amount': order_tx_amount, #normalized
        'destination': pay_destination
    })
    
def _parse_order_expiration_or_cancellation(db, message, cur_block_index, cur_block, isCancellation=True):
    #this all needs to be idempotent, due to the fact that we may be processing off of a reparse

    if not config.AUTO_BTC_ESCROW_ENABLE:
        return
    
    record = db.autobtcescrow_transactions.find_one({'order_tx_hash': message['order_hash']})
    if not record:
        return
    assert record['source_address'] is None or record['source_address'] == message['source']

    assert record['status'] not in ['new', 'invalid', 'filled']
    if record['status'] == 'open':
        tx_info = blockchain.gettransaction(record['btc_deposit_tx_hash'])
        assert tx_info
        assert tx_info['confirmations'] != 0 #that BTC should def be confirmed by now...
        assert record['remaining_amount'] <= tx_info['valueOut']
        assert record['status'] != 'new'
        assert record['source_address']

        #close out the order and refund any remaining BTC
        record['status'] = 'cancelled' if isCancellation else 'expired'
    
        refund_tx_hash = util.call_jsonrpc_api("do_send", {
            'source': record['escrow_address'],
            'destination': record['source_address'],
            'asset': 'BTC',
            'quantity': util_bitcoin.denormalize_quantity(record['remaining_amount'])
        }, abort_on_error=True)['result']
        record['refund_tx_hash'] = refund_tx_hash
        record.save()
        
def parse_order_expiration(db, message, cur_block_index, cur_block):
    return _parse_order_expiration_or_cancellation(db, message, cur_block_index, cur_block, isCancellation=False)

def parse_order_cancellation(db, message, cur_block_index, cur_block):
    return _parse_order_expiration_or_cancellation(db, message, cur_block_index, cur_block, isCancellation=True)
    
def on_new_block(db, cur_block_index, cur_block):
    #see if there are any autobtcescrow transactions we need to make a BTCpay on
    pending_payments = db.autobtcescrow_pending_payments.find({'target_block_index': cur_block_index})
    pending_payments_to_delete = []
    for p in pending_payments:
        order_record = db.autobtcescrow_orders.find_one({'_id': p['autobtcescrow_order_id']})
        assert order_record
        
        #actually make the BTCpay now...
        payment_tx_hash = util.call_jsonrpc_api("do_btcpay", {
            'order_match_id': p['order_match_id']
        }, abort_on_error=True)['result']
        
        #record it
        order_record['funded_order_matches'].append((p['order_match_id'], payment_tx_hash))
        order_record.save()
        pending_payments_to_delete.append(p['_id'])
        
    db.autobtcescrow_pending_payments.remove({'_id': {'$in': pending_payments_to_delete}})
        
    