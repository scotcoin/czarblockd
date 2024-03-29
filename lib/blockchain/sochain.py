'''
chain.sp
'''
import logging

from lib import config, util, util_czarcoin

def get_host():
    if config.BLOCKCHAIN_SERVICE_CONNECT:
        return config.BLOCKCHAIN_SERVICE_CONNECT
    else:
        return 'https://chain.so'

def sochain_network():
	network = config.CZR
	if config.TESTNET:
		network += 'TEST'
	return network

def check():
    pass

def getinfo():
    result = util.get_url(get_host() + '/api/v2/get_info/{}'.format(sochain_network()), abort_on_error=True)
    if 'status' in result and result['status'] == 'success':
        return {
            "info": {
                "blocks": result['data']['blocks']
            }
        }
    else:
    	return None

def listunspent(address):
    result = util.get_url(get_host() + '/api/v2/get_tx_unspent/{}/{}'.format(sochain_network(), address), abort_on_error=True)
    if 'status' in result and result['status'] == 'success':
        utxo = []
        for txo in result['data']['txs']:
            newtxo = {
                'address': address,
                'txid': txo['txid'],
                'vout': txo['output_no'],
                'ts': txo['time'],
                'scriptPubKey': txo['script_hex'],
                'amount': float(txo['value']),
                'confirmations': txo['confirmations'],
                'confirmationsFromCache': False
            }
            utxo.append(newtxo)
        return utxo
    else:
        return None

def getaddressinfo(address):
    infos = util.get_url(get_host() + '/api/v2/address/{}/{}'.format(sochain_network(), address), abort_on_error=True)
    if 'status' in infos and infos['status'] == 'success':
        transactions = []
        for tx in infos['data']['txs']:
            transactions.append(tx['txid'])
        return {
            'addrStr': address,
            'balance': float(infos['data']['balance']),
            'balanceSat': float(infos['data']['balance']) * config.UNIT,
            'totalReceived': float(infos['data']['received_value']),
            'totalReceivedSat': float(infos['data']['received_value']) * config.UNIT,
            'unconfirmedBalance': 0,
            'unconfirmedBalanceSat': 0,
            'unconfirmedTxApperances': 0,
            'txApperances': infos['data']['total_txs'],
            'transactions': transactions
        }
    
    return None

def gettransaction(tx_hash):
    tx = util.get_url(get_host() + '/api/v2/get_tx/{}/{}'.format(sochain_network(), address), abort_on_error=True)
    if 'status' in tx and tx['status'] == 'success':
        valueOut = 0
        for vout in tx['data']['tx']['vout']:
            valueOut += float(vout['value'])
        return {
            'txid': tx_hash,
            'version': tx['data']['tx']['version'],
            'locktime': tx['data']['tx']['locktime'],
            'blockhash': tx['data']['tx']['blockhash'],
            'confirmations': tx['data']['tx']['confirmations'],
            'time': tx['data']['tx']['time'],
            'blocktime': tx['data']['tx']['blocktime'],
            'valueOut': valueOut,
            'vin': tx['data']['tx']['vin'],
            'vout': tx['data']['tx']['vout']
        }

    return None

def get_pubkey_for_address(address):
    #first, get a list of transactions for the address
    address_info = getaddressinfo(address)

    #if no transactions, we can't get the pubkey
    if not address_info['transactions']:
        return None
    
    #for each transaction we got back, extract the vin, pubkey, go through, convert it to binary, and see if it reduces down to the given address
    for tx_id in address_info['transactions']:
        #parse the pubkey out of the first sent transaction
        tx = gettransaction(tx_id)
        pubkey_hex = tx['vin'][0]['script'].split(' ')[1]
        pubkey_hex = tx['vin'][0]['scriptSig']['asm'].split(' ')[1]
        if util_czarcoin.pubkey_to_address(pubkey_hex) == address:
            return pubkey_hex
    return None

