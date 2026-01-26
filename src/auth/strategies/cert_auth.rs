use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::{
    pkcs1::EncodeRsaPrivateKey,
    pkcs8::{DecodePrivateKey, EncodePublicKey},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SnowflakeError, this_errors};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: u64,
    nbf: u64,
    exp: u64,
}

fn hash(pk_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pk_bytes);
    let hashed = hasher.finalize();

    let engine = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::general_purpose::NO_PAD,
    );

    engine.encode(hashed)
}

// Taken from:
// https://github.com/snowflakedb/gosnowflake/blob/master/auth.go#L695
pub(crate) fn create_cert_auth_jwt(
    account_id: &str,
    username: &str,
    pk_pem: &str,
) -> Result<String, SnowflakeError> {
    let pkey = this_errors!(
        "failed to derive private key from pem",
        rsa::RsaPrivateKey::from_pkcs8_pem(pk_pem)
    );

    let pubk = this_errors!(
        "failed to get public key from private key",
        pkey.to_public_key().to_public_key_der()
    );

    let jwt_key = EncodingKey::from_rsa_der(
        this_errors!("failed to convert public key into der", pkey.to_pkcs1_der()).as_bytes(),
    );

    let iss = format!(
        "{}.{}.SHA256:{}",
        account_id,
        username,
        hash(pubk.as_bytes())
    );
    let sub = format!("{}.{}", account_id, username,);
    let iat = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time somehow went backwards")
        .as_secs();

    // This is the unix timestamp of '2015-10-10T12:00:00.000Z'
    // I'm not sure why, but the go library has this date hardcoded
    // https://github.com/snowflakedb/gosnowflake/blob/master/auth.go#L699
    let nbf: u64 = 1_444_478_400_000;
    let exp = iat + 60;

    let claims = Claims {
        iss,
        sub,
        iat,
        nbf,
        exp,
    };

    let header = Header::new(Algorithm::RS256);

    let jwt = this_errors!(
        "failed to encode jwt for certificate authentication",
        jsonwebtoken::encode(&header, &claims, &jwt_key)
    );

    Ok(jwt)
}
