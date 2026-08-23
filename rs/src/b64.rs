//! Strict standard base64 (RFC 4648, with padding).
//!
//! The streaming cassette spec requires readers to reject any `message_b64`
//! containing whitespace or out-of-alphabet characters instead of silently
//! discarding them (as several standard decoders do by default). Hand-rolled
//! over std so strictness is guaranteed rather than a decoder config detail.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn sextet(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Encode to standard base64 with padding, no line wrapping.
pub(crate) fn encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        let idx = [(n >> 18) & 63, (n >> 12) & 63, (n >> 6) & 63, n & 63];
        for (i, v) in idx.into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(ALPHABET[v as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Strict decode: rejects whitespace, out-of-alphabet characters, misplaced
/// or missing padding, lengths not a multiple of 4, and non-zero trailing
/// bits (non-canonical encodings).
pub(crate) fn decode(s: &str) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if !bytes.len().is_multiple_of(4) {
        return Err(format!("length {} is not a multiple of 4", bytes.len()));
    }
    let pad = bytes.iter().rev().take_while(|&&c| c == b'=').count();
    if pad > 2 {
        return Err("more than 2 padding characters".into());
    }
    let body = &bytes[..bytes.len() - pad];
    let mut out = Vec::with_capacity(body.len() * 3 / 4 + 2);
    let mut acc: u32 = 0;
    let mut nbits = 0u32;
    for &c in body {
        let v = sextet(c).ok_or_else(|| {
            format!("invalid character {:?} (whitespace and out-of-alphabet rejected)", c as char)
        })?;
        acc = (acc << 6) | u32::from(v);
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
        }
    }
    // Trailing bits left over from a padded final group must be zero.
    if acc & ((1 << nbits) - 1) != 0 {
        return Err("non-canonical encoding: non-zero trailing bits".into());
    }
    // pad=2 leaves 4 spare bits (2 sextets -> 1 byte), pad=1 leaves 2.
    let expected_spare = match pad {
        2 => 4,
        1 => 2,
        _ => 0,
    };
    if nbits != expected_spare {
        return Err("padding does not match data length".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for msg in [&b""[..], b"a", b"ab", b"abc", b"ping-1", b"blob-chunk 1", &[0u8, 255, 7]] {
            let enc = encode(msg);
            assert!(!enc.contains(char::is_whitespace));
            assert_eq!(decode(&enc).unwrap(), msg, "roundtrip {msg:?}");
        }
        assert_eq!(encode(b"ping-1"), "cGluZy0x");
        assert_eq!(encode(b"chunk-one\n"), "Y2h1bmstb25lCg==");
    }

    #[test]
    fn strict_rejections() {
        for bad in [
            "YmxvYi1jaHV ayAy",  // embedded space, length-valid (16)
            "cGl\nZy0x",         // embedded newline, length-valid
            "cG9uZy0!",          // out-of-alphabet, length-valid
            "cGluZy0x\n",        // trailing newline
            "cG9uZy0",           // length not multiple of 4
            "YQ==YQ==",          // misplaced '=' (padding not at end only)
            "Y===",              // 3 pads
            "YR==",              // non-zero trailing bits
        ] {
            assert!(decode(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn empty_string_is_empty_message() {
        assert_eq!(decode("").unwrap(), Vec::<u8>::new());
        assert_eq!(encode(b""), "");
    }
}
