//! UTC timestamps in the cassette envelope format.
//!
//! `recorded_at` is `YYYY-MM-DDTHH:MM:SSZ`: UTC, second precision, literal
//! `Z`. Std-only; no calendar dependency.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UTC time as `YYYY-MM-DDTHH:MM:SSZ`.
pub(crate) fn now_rfc3339() -> String {
    // A clock before the epoch is clamped to the epoch.
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    format_unix(secs)
}

/// Format Unix seconds as `YYYY-MM-DDTHH:MM:SSZ` (proleptic Gregorian, UTC).
pub(crate) fn format_unix(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Days since 1970-01-01 to (year, month, day). Hinnant's civil_from_days.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_known_instants() {
        // Values cross-checked with `date -u -r <secs>`.
        for (secs, want) in [
            (0, "1970-01-01T00:00:00Z"),
            (86_399, "1970-01-01T23:59:59Z"),
            (951_782_400, "2000-02-29T00:00:00Z"), // leap day, /400 rule
            (4_107_542_400, "2100-03-01T00:00:00Z"), // 2100 not leap
            (1_709_164_799, "2024-02-28T23:59:59Z"),
            (1_709_164_800, "2024-02-29T00:00:00Z"),
            (1_709_251_200, "2024-03-01T00:00:00Z"),
            (1_735_689_599, "2024-12-31T23:59:59Z"),
            (1_735_689_600, "2025-01-01T00:00:00Z"),
            (1_787_486_400, "2026-08-23T12:00:00Z"),
            (-1, "1969-12-31T23:59:59Z"),
        ] {
            assert_eq!(format_unix(secs), want, "secs={secs}");
        }
    }

    #[test]
    fn now_matches_envelope_shape() {
        let s = now_rfc3339();
        let b = s.as_bytes();
        assert_eq!(b.len(), 20, "{s}");
        for (i, c) in b.iter().enumerate() {
            match i {
                4 | 7 => assert_eq!(*c, b'-', "{s}"),
                10 => assert_eq!(*c, b'T', "{s}"),
                13 | 16 => assert_eq!(*c, b':', "{s}"),
                19 => assert_eq!(*c, b'Z', "{s}"),
                _ => assert!(c.is_ascii_digit(), "{s}"),
            }
        }
    }
}
