use crate::error::{IdentityError, Result};
use mtp::crypto::{Keyring, PublicKeyBundle};
use mtp::files::{
    FileError, load_keyring, load_public_key_bundle, save_keyring, save_public_key_bundle,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub const KEYRING_PATH: &str = "./omega.mk";
pub const PUBLIC_KEY_PATH: &str = "./omega.mpkb";

pub struct OmegaIdentity {
    keyring: Arc<Keyring>,
}

static PUBLIC_BUNDLE_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

impl OmegaIdentity {
    pub fn load_or_create(passphrase: &[u8]) -> Result<Self> {
        Self::load_or_create_at(
            Path::new(KEYRING_PATH),
            Path::new(PUBLIC_KEY_PATH),
            passphrase,
        )
    }

    pub(crate) fn load_or_create_at(
        keyring_path: impl AsRef<Path>,
        public_key_path: impl AsRef<Path>,
        passphrase: &[u8],
    ) -> Result<Self> {
        let keyring_path = keyring_path.as_ref();
        let public_key_path = public_key_path.as_ref();
        let keyring = match load_keyring(keyring_path, passphrase) {
            Ok(keyring) => keyring,
            Err(FileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Self::create_at(keyring_path, public_key_path, passphrase);
            }
            Err(error) => {
                return Err(IdentityError::Storage {
                    path: keyring_path.to_path_buf(),
                    source: error,
                }
                .into());
            }
        };

        let identity = Self {
            keyring: Arc::new(keyring),
        };
        match load_public_key_bundle(public_key_path) {
            Ok(persisted_bundle) => {
                identity.verify_public_bundle(public_key_path, &persisted_bundle)?
            }
            Err(FileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Self::persist_public_bundle(&identity.public_key_bundle(), public_key_path)?;
            }
            Err(error) => {
                return Err(IdentityError::Storage {
                    path: public_key_path.to_path_buf(),
                    source: error,
                }
                .into());
            }
        }
        Ok(identity)
    }

    fn create_at(keyring_path: &Path, public_key_path: &Path, passphrase: &[u8]) -> Result<Self> {
        let keyring = Keyring::generate();
        save_keyring(&keyring, keyring_path, passphrase).map_err(|error| {
            IdentityError::Storage {
                path: keyring_path.to_path_buf(),
                source: error,
            }
        })?;
        Self::persist_public_bundle(&keyring.public_key_bundle(), public_key_path)?;
        Ok(Self {
            keyring: Arc::new(keyring),
        })
    }

    fn persist_public_bundle(bundle: &PublicKeyBundle, path: &Path) -> Result<()> {
        let temporary_path = temporary_path(path)?;
        let result = save_public_key_bundle(bundle, &temporary_path).map_err(|source| {
            IdentityError::Storage {
                path: path.to_path_buf(),
                source,
            }
        });
        if let Err(error) = result {
            let _ = fs::remove_file(&temporary_path);
            return Err(error.into());
        }
        if let Err(source) = fs::File::open(&temporary_path).and_then(|file| file.sync_all()) {
            let _ = fs::remove_file(&temporary_path);
            return Err(IdentityError::Io {
                path: path.to_path_buf(),
                source,
            }
            .into());
        }
        if let Err(source) = fs::rename(&temporary_path, path) {
            let _ = fs::remove_file(&temporary_path);
            return Err(IdentityError::Io {
                path: path.to_path_buf(),
                source,
            }
            .into());
        }
        Ok(())
    }

    fn verify_public_bundle(&self, path: &Path, persisted_bundle: &PublicKeyBundle) -> Result<()> {
        let expected = self.keyring.public_key_bundle().try_as_bytes()?;
        let actual = persisted_bundle.try_as_bytes()?;
        if expected != actual {
            return Err(IdentityError::PublicBundleMismatch {
                path: path.to_path_buf(),
            }
            .into());
        }
        Ok(())
    }

    pub fn keyring(&self) -> &Keyring {
        &self.keyring
    }

    pub fn public_key_bundle(&self) -> PublicKeyBundle {
        self.keyring().public_key_bundle()
    }

    pub fn clone_keyring(&self) -> Result<Keyring> {
        let bytes = self.keyring.try_to_bytes()?;
        Ok(Keyring::from_bytes(&bytes)?)
    }

    #[cfg(test)]
    pub(crate) fn from_keyring(keyring: Keyring) -> Self {
        Self {
            keyring: Arc::new(keyring),
        }
    }
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().ok_or_else(|| IdentityError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "identity path has no file name",
        ),
    })?;
    let mut temporary_name = file_name.to_os_string();
    temporary_name.push(format!(
        ".tmp-{}-{}",
        std::process::id(),
        PUBLIC_BUNDLE_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    Ok(parent.join(temporary_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_directory() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("omega-identity-test-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).expect("create test directory");
        path
    }

    #[test]
    fn generated_identity_survives_restart_with_separate_file_formats() {
        let directory = test_directory();
        let keyring_path = directory.join("omega.mk");
        let public_key_path = directory.join("omega.mpkb");
        let passphrase = b"test-passphrase";

        let first = OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase)
            .expect("create identity");
        let first_bundle = first.public_key_bundle().try_as_bytes().expect("bundle");

        let keyring_bytes = fs::read(&keyring_path).expect("read keyring");
        let bundle_bytes = fs::read(&public_key_path).expect("read bundle");
        assert_eq!(&keyring_bytes[..4], b"MTMK");
        assert_eq!(&bundle_bytes[..4], b"MPKB");

        let restarted =
            OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase)
                .expect("reload identity");
        assert_eq!(
            restarted
                .public_key_bundle()
                .try_as_bytes()
                .expect("bundle"),
            first_bundle
        );
        assert_eq!(
            load_public_key_bundle(&public_key_path)
                .expect("load public bundle")
                .try_as_bytes()
                .expect("bundle"),
            first_bundle
        );

        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn invalid_existing_keyring_does_not_create_a_new_identity() {
        let directory = test_directory();
        let keyring_path = directory.join("omega.mk");
        let public_key_path = directory.join("omega.mpkb");
        fs::write(&keyring_path, b"not-a-keyring").expect("write invalid keyring");

        let result =
            OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, b"test-passphrase");
        assert!(result.is_err());
        assert!(!public_key_path.exists());

        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn missing_public_bundle_is_repaired_without_changing_keyring() {
        let directory = test_directory();
        let keyring_path = directory.join("omega.mk");
        let public_key_path = directory.join("omega.mpkb");
        let passphrase = b"test-passphrase";

        OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase)
            .expect("create identity");
        let original_keyring = fs::read(&keyring_path).expect("read keyring");
        fs::remove_file(&public_key_path).expect("remove bundle");

        let repaired =
            OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase)
                .expect("repair bundle");

        assert_eq!(
            fs::read(&keyring_path).expect("read keyring"),
            original_keyring
        );
        assert_eq!(
            repaired.public_key_bundle().try_as_bytes().expect("bundle"),
            load_public_key_bundle(&public_key_path)
                .expect("load bundle")
                .try_as_bytes()
                .expect("bundle")
        );

        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn mismatched_public_bundle_stops_startup() {
        let directory = test_directory();
        let keyring_path = directory.join("omega.mk");
        let public_key_path = directory.join("omega.mpkb");
        let passphrase = b"test-passphrase";

        OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase)
            .expect("create identity");
        let other_keyring = Keyring::generate();
        save_public_key_bundle(&other_keyring.public_key_bundle(), &public_key_path)
            .expect("save mismatched bundle");

        assert!(matches!(
            OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase),
            Err(crate::OmegaError::Identity(
                IdentityError::PublicBundleMismatch { .. }
            ))
        ));

        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn wrong_passphrase_does_not_replace_existing_keyring() {
        let directory = test_directory();
        let keyring_path = directory.join("omega.mk");
        let public_key_path = directory.join("omega.mpkb");
        let passphrase = b"test-passphrase";

        OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, passphrase)
            .expect("create identity");
        let original_keyring = fs::read(&keyring_path).expect("read keyring");

        assert!(
            OmegaIdentity::load_or_create_at(&keyring_path, &public_key_path, b"wrong-passphrase")
                .is_err()
        );
        assert_eq!(
            fs::read(&keyring_path).expect("read keyring"),
            original_keyring
        );

        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
