use dashmap::DashMap;
use once_cell::sync::Lazy;
use rand::{Rng, thread_rng};

static LINKS: Lazy<DashMap<String, String>> = Lazy::new(DashMap::new);

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPRSTUVWXYZ#1234567890";

pub async fn add_short_link(long: &str) -> Result<String, ()> {
    let raw = generate_unique_short_link().await;
    LINKS.insert(raw.clone(), long.to_string());

    Ok(format!(
        "omega.tensamin.net/direct/{}",
        format_with_dashes(&raw)
    ))
}

async fn generate_unique_short_link() -> String {
    loop {
        let short = generate_short_link().await;
        if !LINKS.contains_key(&short) {
            return short;
        }
    }
}

pub async fn generate_short_link() -> String {
    let len = short_length();

    let mut rng = thread_rng();
    (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

pub async fn get_short_link(short: &str) -> Result<String, ()> {
    let normalized = normalize_short(short);

    LINKS.get(&normalized).map(|v| v.value().clone()).ok_or(())
}

/* ---------------- helpers ---------------- */

fn short_length() -> usize {
    let count = LINKS.len();

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
