use crate::db::short_link_repo;
use rand::RngExt;

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPRSTUVWXYZ1234567890";

pub async fn add_short_link(long: &str) -> Result<String, ()> {
    loop {
        let raw = generate_short_link().await;
        if short_link_repo::insert(&raw, long).await.map_err(|_| ())? {
            let public_urls = crate::config::public_urls().map_err(|_| ())?;
            return Ok(format!(
                "{}/direct/{}",
                public_urls.omega_base_url,
                format_with_dashes(&raw)
            ));
        }
    }
}

pub async fn generate_short_link() -> String {
    let len = short_length().await;

    let mut rng = rand::rng();
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
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

async fn short_length() -> usize {
    let count = short_link_repo::count().await.unwrap_or(0);

    match count {
        0..=1_999 => 4,
        2_000..=999_999 => 8,
        _ => 12,
    }
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
