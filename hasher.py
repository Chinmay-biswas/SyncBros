import hashlib

def hash_chunk(data):
    return hashlib.sha256(data).hexdigest()