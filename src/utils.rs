
/// Quotes a snowflake identifier to ensure proper casing is maintained.<br />
/// For example, `quote_ident("users") == "\"users\""`
pub fn quote_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for ch in name.chars() {
        if ch == '"' {
            out.push('"');
            out.push('"');
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

/// Converts catalog, schema and table into their quoted representation.<br />
/// For example, `qualify_table(None, "auth", "users") == "\"auth\".\"users\""`
pub fn qualify_table(catalog: Option<&str>, schema: Option<&str>, table: &str) -> String {
    let mut parts = Vec::with_capacity(3);
    if let Some(c) = catalog {
        parts.push(quote_ident(c));
    }
    if let Some(s) = schema {
        parts.push(quote_ident(s));
    }
    parts.push(quote_ident(table));
    parts.join(".")
}
