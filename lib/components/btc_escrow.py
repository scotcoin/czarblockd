import os
import logging
import decimal
import base64
import json
from datetime import datetime

from lib import config, util, util_bitcoin, blockchain

def parse_order(db, message, cur_block_index, cur_block):
    if not config.AUTO_BTC_ESCROW_ENABLE:
        return
    
    #determine if we have an auto btc escrow record for this order and merge in information
    record = db.autobtcescrow_transactions.find_one({'order_tx_hash': message['tx_hash']})
    if not record:
        return
    
    if record['status'] != 'new': #already processed on an earlier parse through
        return
    
    #ensure that the owner of this order was the one that actually submitted this record
    #verifymessage params: <bitcoinaddress> <signature> <message>
    verify_result = util.call_jsonrpc_api('verifymessage',
        params=[message['source'], record['signed_order_tx_hash'], record['order_tx_hash']],
        endpoint=config.BACKEND_RPC, auth=config.BACKEND_AUTH, abort_on_error=True)
    loggng.warn("AutoBTCEscrow: VERIFICATION RESULT: %s" % (verify_result))
    if not verify_result or verify_result == 'false':
        logging.warn("AutoBTCEscrow: Identity verification failed for escrow record '%s'" % record['_id'])
        record['status'] == 'invalid'
        record.save()
        return #user that made the escrow record could not prove that they were the same one that placed the order

    #make sure the specified BTC transaction hash exists and matches required criteria
    btc_tx_info = blockchain.gettransaction(record['btc_deposit_tx_hash'])
    if not btc_tx_info:
        logging.warn("AutoBTCEscrow: Cited BTC deposit txhash (%s) doesn't exist for escrow record '%s'" % (
            message['btc_deposit_tx_hash'], record['_id']))
        record['status'] == 'invalid'
        record.save()
        return
    #(already known this isn't associated with more than this order)
    #amounts must match of what the order calls for and what the deposit provides, with an output going to the escrow address
    assert message['give_asset'] == 'BTC'
    required_escrow_amount = util_bitcoin.normalize_quantity(message['give_quantity'])
    for output in btc_tx_info['vout']:
        if     output['scriptPubKey']['reqSigs'] == 1 \
           and output['scriptPubKey']['type'] == 'pubkeyhash' \
           and len(output['scriptPubKey']['addresses']) == 1 \
           and output['scriptPubKey']['addresses'][0] == record['escrow_address'] \
           and output['scriptPubKey']['value'] == required_escrow_amount:
            break #found the output...
    else: #didn't find the output
        logging.warn("AutoBTCEscrow: Could not find suitable txout in BTC tx hash '%s' for escrow record '%s'" % (
            message['btc_deposit_tx_hash'], record['_id']))
        record['status'] == 'invalid'
        record.save()
        return
    
    record['status'] = 'open'
    record['order_expire_index'] = message['expire_index']
    assert record['order_expire_index'] == message['expire_index']
    record['remaining_amount'] = required_escrow_amount #normalized
    logging.info("AutoBTCEsrow: Escrow record '%s' successfully created (BTCTxHash: '%s')" % (
        record['_id'], message['btc_deposit_tx_hash']))
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
    if record['status'] != 'open': #skip on reparse
        return

    assert record['order_tx_hash'] in [message['tx0_hash'], message['tx1_hash']]
    assert record['source_address'] == (message['tx0_address'] if record['order_tx_hash'] == message['tx0_hash'] else message['tx1_address'])
    assert record['remaining_amount'] > 0 #status would be closed otherwise
    
    match_btc_amount= util_bitcoin.normalize_quantity(
        message['forward_quantity'] if message['forward_asset'] == 'BTC' else message['backward_quantity'])
    if match_btc_amount > record['remaining_amount']:
        logging.warn("AutoBTCEscrow: Order match for %s BTC exceed remaining amount in escrow (%s BTC) for escrow record '%s'" % (
            match_btc_amount, record['remaining_amount'], record['_id']))
        record['status'] == 'invalid' #don't do anything, as the BTC can be refunded via a cancel tx, or order expiration....
        record.save()
        return
        
    tx_info = blockchain.gettransaction(record['btc_deposit_tx_hash'])
    assert tx_info and tx_info['confirmations'] != 0 #the BTC deposit TX must have at least 1 confirm to pay out on it
    
    #set up to make the btcpay in N blocks
    pay_destination = message['tx1_address'] if record['order_tx_hash'] == message['tx0_hash'] else message['tx0_address']
    db.autobtcescrow_pending_payments.insert({
        'target_block_index': cur_block_index + config.AUTOBTCESCROW_NUM_BLOCKS_FOR_BTCPAY,
        'autobtcescrow_order_id': record.id,
        'order_match_id': message['tx0_hash'] + message['tx1_hash'],
        'amount': match_btc_amount, #normalized
        'destination': pay_destination
    })
    
def _parse_order_expiration_or_cancellation(db, message, cur_block_index, cur_block, isCancellation=True):
    #this all needs to be idempotent, due to the fact that we may be processing off of a reparse

    if not config.AUTO_BTC_ESCROW_ENABLE:
        return
    
    record = db.autobtcescrow_transactions.find_one({'order_tx_hash': message['order_hash']})
    if not record:
        return
    
    if record['status'] in ['new', 'filled', 'expired', 'cancelled']:
        return
    assert record['status'] in ['open', 'invalid']

    tx_info = blockchain.gettransaction(record['btc_deposit_tx_hash'])    

    do_refund = False
    if record['status'] == 'open':
        assert tx_info and tx_info['confirmations'] > 0 #that BTC should def be confirmed by now...
        assert record['remaining_amount'] <= tx_info['valueOut']
        assert record['status'] != 'new'
        assert record['source_address']
        do_refund = True
    elif     record['status'] == 'invalid' \
         and record['remaining_amount'] > 0 \
         and tx_info and tx_info['confirmations'] > 0:
        do_refund = True

    if do_refund:    
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

        #remove the pending payment entry now just in case we get an error on future API queries in this loop...
        db.autobtcescrow_pending_payments.remove({'_id': p['_id']})
