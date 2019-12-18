import base64
import os

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def generate_key(password):
    if type(password) == str:
        password = password.encode('utf-8')

    salt = os.urandom(16)
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=1000000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(password))


def generate_fernet(key):
    return Fernet(key)


def encrypt(data, fernet_func):
    return fernet_func.encrypt(data)


def decrypt(data, fernet_func):
    return fernet_func.decrypt(data)
