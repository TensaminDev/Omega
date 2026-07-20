use crate::error::{OmegaError, Result};

pub fn parse_positive_id(value: &str) -> Result<i64> {
    let id = value
        .parse::<i64>()
        .map_err(|_| OmegaError::Validation("ID must be a positive integer".to_string()))?;
    if id <= 0 {
        return Err(OmegaError::Validation(
            "ID must be a positive integer".to_string(),
        ));
    }
    Ok(id)
}

pub fn validate_non_empty(value: &str, field: &str, max_len: usize) -> Result<()> {
    let length = value.chars().count();
    if length == 0 || length > max_len {
        return Err(OmegaError::Validation(format!(
            "{field} must contain 1 to {max_len} characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_positive_id, validate_non_empty};

    #[test]
    fn rejects_invalid_ids() {
        assert!(parse_positive_id("bad").is_err());
        assert!(parse_positive_id("0").is_err());
        assert_eq!(parse_positive_id("7").expect("valid test ID"), 7);
    }

    #[test]
    fn enforces_string_bounds() {
        assert!(validate_non_empty("", "username", 15).is_err());
        assert!(validate_non_empty("abcdefghijklmnop", "username", 15).is_err());
        assert!(validate_non_empty("alice", "username", 15).is_ok());
    }
}
