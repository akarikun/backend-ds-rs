use super::config::Config;
use aes::Aes256;
use aes::cipher::BlockDecryptMut;
use aes::cipher::block_padding::Pkcs7;
use aes::cipher::{BlockEncryptMut, KeyIvInit};
use base64::{Engine as _, engine::general_purpose};
use cbc::Decryptor;
use md5::{Digest, Md5};
use rand::Rng;
use std::sync::LazyLock;

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| Config::load().unwrap());

type Aes256CbcDec = Decryptor<Aes256>;
type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

pub fn get_aes_passphrase() -> String {
    CONFIG.aes_passphrase.clone()
}

fn evp_bytes_to_key(password: &[u8], salt: &[u8]) -> ([u8; 32], [u8; 16]) {
    let mut hasher = Md5::new();
    let mut key_iv = Vec::new();
    let mut derived_bytes = Vec::new();

    while key_iv.len() < 48 {
        // 32 bytes for key + 16 bytes for IV
        if !derived_bytes.is_empty() {
            hasher.update(&derived_bytes);
        }
        hasher.update(password);
        hasher.update(salt);
        derived_bytes = hasher.finalize_reset().to_vec();
        key_iv.extend_from_slice(&derived_bytes);
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&key_iv[0..32]);

    let mut iv = [0u8; 16];
    iv.copy_from_slice(&key_iv[32..48]);

    (key, iv)
}

pub fn decrypt_cryptojs_aes(encrypted_base64: &str, passphrase: &str) -> Result<String, String> {
    // Decode Base64 input
    let encrypted_data = general_purpose::STANDARD
        .decode(encrypted_base64)
        .map_err(|e| format!("Base64 decode error: {}", e))?;

    // Validate data format and extract salt and ciphertext
    if encrypted_data.len() < 16 || &encrypted_data[0..8] != b"Salted__" {
        return Err("Invalid encrypted data format: missing 'Salted__' prefix".to_string());
    }
    let salt = &encrypted_data[8..16];
    let ciphertext = &encrypted_data[16..];

    // Derive key and IV
    let (key, iv) = evp_bytes_to_key(passphrase.as_bytes(), salt);

    // Initialize AES-256-CBC decryptor
    let cipher = Aes256CbcDec::new(&key.into(), &iv.into());

    // Decrypt with PKCS#7 padding
    let mut buf = ciphertext.to_vec();
    let decrypted_bytes = cipher
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|e| format!("Decryption error: {}", e))?;

    // Convert decrypted bytes to UTF-8 string
    String::from_utf8(decrypted_bytes.to_vec())
        .map_err(|e| format!("UTF-8 conversion error: {}", e))
}

pub fn encrypt_cryptojs_aes(plaintext: &str, passphrase: &str) -> Result<String, String> {
    // Validate inputs
    if plaintext.is_empty() {
        return Err("Empty plaintext".to_string());
    }
    if passphrase.is_empty() {
        return Err("Empty passphrase".to_string());
    }

    // Generate random 8-byte salt
    let mut salt = [0u8; 8];
    rand::rng().fill_bytes(&mut salt);

    // Derive key and IV
    let (key, iv) = evp_bytes_to_key(passphrase.as_bytes(), &salt);

    // Debug: Print inputs and derived values
    // eprintln!("Plaintext: {}", plaintext);
    // eprintln!("Plaintext length: {}", plaintext.len());
    // eprintln!(
    //     "Plaintext bytes (hex): {}",
    //     hex::encode(plaintext.as_bytes())
    // );
    // eprintln!("Salt (hex): {}", hex::encode(&salt));
    // eprintln!("Key (hex): {}", hex::encode(&key));
    // eprintln!("IV (hex): {}", hex::encode(&iv));

    // Initialize AES-256-CBC encryptor
    let cipher = Aes256CbcEnc::new(&key.into(), &iv.into());

    // Encrypt with PKCS#7 padding, using encrypt_padded_vec_mut to handle buffer
    let encrypted_bytes = cipher.encrypt_padded_vec_mut::<Pkcs7>(plaintext.as_bytes());

    // Debug: Print encrypted bytes
    // eprintln!("Encrypted bytes (hex): {}", hex::encode(&encrypted_bytes));

    // Construct output: Salted__ + salt + ciphertext
    let mut output = Vec::with_capacity(8 + 8 + encrypted_bytes.len());
    output.extend_from_slice(b"Salted__");
    output.extend_from_slice(&salt);
    output.extend_from_slice(&encrypted_bytes);

    // Encode to Base64
    let base64_output = general_purpose::STANDARD.encode(&output);
    // eprintln!("Encrypted Base64: {}", base64_output);
    Ok(base64_output)
}
