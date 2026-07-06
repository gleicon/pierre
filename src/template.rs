use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Masks variable tokens (numbers, hex) in a log line and hashes the resulting
/// signature into a template id — a simplified stand-in for Drain's prefix-tree
/// clustering, grouping lines that share the same shape under one id.
pub fn template_id(message: &str) -> u64 {
    let masked: Vec<String> = message.split_whitespace().map(mask_token).collect();
    let mut hasher = DefaultHasher::new();
    masked.join(" ").hash(&mut hasher);
    hasher.finish()
}

/// Masks variable tokens to a placeholder; literal (constant) tokens pass through
/// lowercased so the signature reflects the line's shape, not its exact wording.
fn mask_token(token: &str) -> String {
    if token.is_empty() {
        return String::new();
    }
    if token.len() >= 8 && token.chars().all(|c| c.is_ascii_hexdigit()) {
        return "<HEX>".to_string();
    }
    if token.chars().any(|c| c.is_ascii_digit()) {
        return "<NUM>".to_string();
    }
    token.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_shape_same_template() {
        let a = template_id("request 12345 took 42ms");
        let b = template_id("request 98765 took 17ms");
        assert_eq!(a, b, "same shape should share a template id");
    }

    #[test]
    fn different_shape_different_template() {
        let a = template_id("request 12345 took 42ms");
        let b = template_id("user logged in");
        assert_ne!(a, b);
    }
}
