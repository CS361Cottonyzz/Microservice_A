"""
Microservice A: Transaction Filtering Service
Listens on a ZeroMQ REP socket, accepts JSON requests to:
  - Filter transactions by month ("YYYY-MM") and type ("income"/"expense")
  - Lookup a single transaction by its ID
Responds with JSON: either a list of transactions or a single transaction object.
"""
import zmq
import json
from bson.objectid import ObjectId
from db_connection import get_db

# Configuration
ZMQ_HOST = '0.0.0.0'
ZMQ_PORT = 5555

# Setup MongoDB
db = get_db()
transactions = db.transactions

# Setup ZeroMQ REP socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind(f"tcp://{ZMQ_HOST}:{ZMQ_PORT}")
print(f"[Microservice] Listening on tcp://{ZMQ_HOST}:{ZMQ_PORT}...")

while True:
    try:
        # Receive request
        message = socket.recv_json()
        response = None

        # If 'id' provided, fetch single transaction
        txn_id = message.get('id')
        if txn_id:
            try:
                obj_id = ObjectId(txn_id)
                txn = transactions.find_one({'_id': obj_id}, {'_id': 1, 'date': 1, 'type':1, 'description':1, 'category':1, 'amount':1})
                if txn:
                    # Convert ObjectId to string
                    txn['_id'] = str(txn['_id'])
                    response = txn
                else:
                    response = {'error': 'Transaction not found'}
            except Exception as e:
                response = {'error': f'Invalid id: {e}'}
        else:
            # Month+type filter
            month = message.get('month')
            tx_type = message.get('type')
            if not month or not tx_type:
                response = {'error': 'Must provide either "id" or both "month" and "type"'}
            else:
                # Build query: date starts with month, type matches
                regex = f"^{month}"
                query = {'date': {'$regex': regex}, 'type': tx_type}
                cursor = transactions.find(query, {'_id':1, 'date':1, 'type':1, 'description':1, 'category':1, 'amount':1})
                result_list = []
                for txn in cursor:
                    txn['_id'] = str(txn['_id'])
                    result_list.append(txn)
                response = result_list

        # Send JSON response
        socket.send_json(response)
    except Exception as exc:
        # On unexpected errors, reply with error details
        error_msg = {'error': f'{exc}'}
        socket.send_json(error_msg)
