use crate::db::short_link_repo;
use rand::RngExt;

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPRSTUVWXYZ1234567890";

fn validate_target(long: &str, max_bytes: usize) -> crate::error::Result<()> {
    let uri: http::Uri = long
        .parse()
        .map_err(|_| crate::OmegaError::Validation("invalid short-link target".into()))?;
    if long.len() > max_bytes
        || uri.scheme_str() != Some("https")
        || uri
            .authority()
            .is_none_or(|authority| authority.host().is_empty() || authority.as_str().contains('@'))
    {
        return Err(crate::OmegaError::Validation(
            "invalid short-link target".into(),
        ));
    }
    Ok(())
}

pub async fn add_short_link(
    creator: i64,
    long: &str,
    config: &crate::config::OmegaConfig,
) -> crate::error::Result<String> {
    validate_target(long, config.max_short_link_target_bytes)?;
    let len = short_length().await?;
    for _ in 0..config.short_link_insert_attempts {
        let raw: String = {
            let mut rng = rand::rng();
            (0..len)
                .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
                .collect()
        };
        if short_link_repo::insert(creator, &raw, long, config.max_short_links_per_omikron).await? {
            let public_urls = crate::config::public_urls()
                .map_err(|error| crate::OmegaError::Validation(error.to_string()))?;
            return Ok(format!(
                "{}/direct/{}",
                public_urls.omega_base_url,
                format_with_dashes(&raw)
            ));
        }
    }
    Err(crate::OmegaError::Transport(
        "short-link key allocation exhausted".into(),
    ))
}

pub async fn get_short_link(short: &str) -> Result<String, ()> {
    let key = if short.contains("/") {
        short.split("/").nth(1).unwrap_or_default()
    } else {
        short
    };
    let frag = short.replace(key, "");
    let normalized = normalize_short(key);

    if let Some(target) = crate::db::invitation_repo::direct_target(&normalized)
        .await
        .map_err(|_| ())?
    {
        return Ok(target);
    }
    let target = short_link_repo::get(&normalized)
        .await
        .map_err(|_| ())?
        .ok_or(())?;
    Ok(format!("{}{}", target, frag))
}

/* ---------------- helpers ---------------- */

async fn short_length() -> crate::error::Result<usize> {
    let count = short_link_repo::count().await?;

    Ok(match count {
        0..=1_999 => 4,
        2_000..=999_999 => 8,
        _ => 12,
    })
}

fn format_with_dashes(s: &str) -> String {
    s.chars()
        .collect::<Vec<_>>()
        .chunks(4)
        .map(|c| c.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join("-")
}

fn normalize_short(input: &str) -> String {
    input
        .chars()
        .filter(|c| *c != '-')
        .map(|c| match c {
            'Q' | 'O' => '0',
            'I' => 'l',
            _ => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::validate_target;

    #[test]
    fn short_link_targets_allow_https_only() {
        assert!(validate_target("https://example.org/path", 2048).is_ok());
        for target in [
            "javascript:alert(1)",
            "file:///etc/passwd",
            "http://example.org",
            "https://user@example.org",
        ] {
            assert!(validate_target(target, 2048).is_err(), "{target}");
        }
        assert!(validate_target(&"https://example.org/".repeat(200), 2048).is_err());
    }
}
