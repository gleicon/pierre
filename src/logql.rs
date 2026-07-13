use std::collections::BTreeMap;

/// A deliberate subset of LogQL — full LogQL is explicitly out of scope (SPEC.md
/// non-goal). Supports `{label="value", ...}` selectors with an optional trailing
/// grep-equivalent line filter: `{label="value"} |= "text"`.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedQuery {
    pub selector: BTreeMap<String, String>,
    pub line_filter: Option<String>,
}

/// Parses a bare `{label="value", ...}` label set — the same syntax LogQL selectors
/// use, and also what Loki's protobuf push format uses for `StreamAdapter.labels`
/// (a formatted string, not a map, on the wire). Shared so both callers agree on
/// exactly one parser for this syntax.
pub fn parse_label_set(s: &str) -> Result<BTreeMap<String, String>, String> {
    let s = s.trim();
    let open = s
        .find('{')
        .ok_or_else(|| "label set must start with '{', e.g. {level=\"error\"}".to_string())?;
    let close = s
        .find('}')
        .ok_or_else(|| "unterminated label set — missing '}'".to_string())?;
    if close < open {
        return Err("malformed label set".to_string());
    }

    let mut labels = BTreeMap::new();
    for pair in s[open + 1..close].split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (key, rest) = pair
            .split_once('=')
            .ok_or_else(|| format!("malformed label term {pair:?}, expected key=\"value\""))?;
        let value = rest
            .trim()
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .ok_or_else(|| format!("label value must be quoted: {rest:?}"))?;
        labels.insert(key.trim().to_string(), value.to_string());
    }
    Ok(labels)
}

pub fn parse(query: &str) -> Result<ParsedQuery, String> {
    let query = query.trim();
    let close = query
        .find('}')
        .ok_or_else(|| "unterminated label selector — missing `}`".to_string())?;
    let selector = parse_label_set(&query[..=close])?;

    let remainder = query[close + 1..].trim();
    let line_filter = if remainder.is_empty() {
        None
    } else {
        let after_op = remainder
            .strip_prefix("|=")
            .ok_or_else(|| {
                format!("unsupported line-filter operator (only `|=` is supported): {remainder:?}")
            })?
            .trim();
        let text = after_op
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .ok_or_else(|| format!("line filter value must be quoted: {after_op:?}"))?;
        Some(text.to_string())
    };

    Ok(ParsedQuery {
        selector,
        line_filter,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_bare_selector() {
        let parsed = parse(r#"{level="error"}"#).unwrap();
        assert_eq!(parsed.selector.get("level"), Some(&"error".to_string()));
        assert_eq!(parsed.line_filter, None);
    }

    #[test]
    fn parses_multiple_selector_terms() {
        let parsed = parse(r#"{level="error", service="payments"}"#).unwrap();
        assert_eq!(parsed.selector.len(), 2);
        assert_eq!(
            parsed.selector.get("service"),
            Some(&"payments".to_string())
        );
    }

    #[test]
    fn parses_selector_with_line_filter() {
        let parsed = parse(r#"{level="error"} |= "timeout""#).unwrap();
        assert_eq!(parsed.line_filter, Some("timeout".to_string()));
    }

    #[test]
    fn rejects_missing_braces() {
        assert!(parse("level=\"error\"").is_err());
    }

    #[test]
    fn rejects_unsupported_operator() {
        assert!(parse(r#"{level="error"} |~ "regex.*""#).is_err());
    }

    #[test]
    fn rejects_unquoted_value() {
        assert!(parse("{level=error}").is_err());
    }
}
