use std::fmt::Write;

pub fn construct_url(
    base_url: &str,
    params: &[(String, String)],
) -> Result<String, std::fmt::Error> {
    let guessed_length = base_url.len() + (params.len() * 20);
    let mut url = String::with_capacity(guessed_length);

    url.push_str(base_url);

    if params.is_empty() {
        return Ok(url);
    }

    let mut prefix = "?";

    for (key, value) in params {
        url.push_str(prefix);

        encode_into(&mut url, key);
        url.push('=');
        encode_into(&mut url, value);

        prefix = "&";
    }

    Ok(url)
}

fn encode_into(buffer: &mut String, input: &str) -> Result<(), std::fmt::Error> {
    for b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                buffer.push(*b as char);
            }
            _ => {
                write!(buffer, "%{:02X}", b)?;
            }
        }
    }

    Ok(())
}
