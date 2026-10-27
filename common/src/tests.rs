use super::*;
use tonic::metadata::{MetadataMap, MetadataValue};

fn metadata_with(auth: &str) -> MetadataMap {
    let mut metadata = MetadataMap::new();
    metadata.insert(AUTHORIZATION_HEADER, MetadataValue::try_from(auth).unwrap());
    metadata
}

#[test]
fn token_round_trips_subject_and_role() {
    let token = create_access_token("secret", "alice", ROLE_USER, 60).expect("token");
    let claims = decode_access_token(&token, "secret").expect("decode");

    assert_eq!(claims.sub, "alice");
    assert_eq!(claims.role, ROLE_USER);
    assert!(claims.exp > claims.iat);
}

#[test]
fn token_signed_with_other_secret_is_rejected() {
    let token = create_access_token("secret", "alice", ROLE_USER, 60).expect("token");
    assert!(decode_access_token(&token, "other-secret").is_err());
}

#[test]
fn authorize_metadata_requires_valid_bearer_token() {
    let token = create_access_token("secret", "dn-1", ROLE_DATANODE, 60).expect("token");

    let claims = authorize_metadata(&metadata_with(&format!("Bearer {token}")), "secret")
        .expect("authorized");
    assert_eq!(claims.sub, "dn-1");

    for bad in [token.as_str(), "Bearer ", "Bearer not-a-jwt"] {
        let status = authorize_metadata(&metadata_with(bad), "secret").unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated, "{bad}");
    }
    let status = authorize_metadata(&MetadataMap::new(), "secret").unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[test]
fn require_role_denies_other_roles() {
    let token = create_access_token("secret", "alice", ROLE_USER, 60).expect("token");
    let claims = decode_access_token(&token, "secret").expect("decode");

    assert!(require_role(&claims, ROLE_USER).is_ok());
    let status = require_role(&claims, ROLE_MASTER).unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}
