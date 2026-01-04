from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Protocol.KDF import PBKDF2
import os

class AESEncryption:
    
    @staticmethod
    def encrypt_data(data: bytes, key: bytes) -> bytes:
        cipher = AES.new(key, AES.MODE_GCM)
        ciphertext, tag = cipher.encrypt_and_digest(data)
        
        return cipher.nonce + tag + ciphertext
    
    @staticmethod
    def decrypt_data(encrypted_data: bytes, key: bytes) -> bytes:
        nonce = encrypted_data[:16]
        tag = encrypted_data[16:32]
        ciphertext = encrypted_data[32:]
        
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        data = cipher.decrypt_and_verify(ciphertext, tag)
        
        return data
    
    @staticmethod
    def derive_key(password: str, salt: bytes = None) -> tuple:
        if salt is None:
            salt = get_random_bytes(32)
        
        key = PBKDF2(password, salt, dkLen=32, count=100000)
        return key, salt

def encrypt_file(input_path: str, output_path: str, key: bytes):
    with open(input_path, 'rb') as f:
        data = f.read()
    
    encrypted = AESEncryption.encrypt_data(data, key)
    
    with open(output_path, 'wb') as f:
        f.write(encrypted)

def decrypt_file(input_path: str, output_path: str, key: bytes):
    with open(input_path, 'rb') as f:
        encrypted_data = f.read()
    
    decrypted = AESEncryption.decrypt_data(encrypted_data, key)
    
    with open(output_path, 'wb') as f:
        f.write(decrypted)
