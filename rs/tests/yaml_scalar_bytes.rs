//! Pins the YAML writer's scalar rendering. Cassettes are cross-language
//! files, so quoting/block-scalar choices must not drift with the YAML
//! backend. Expected strings were captured from the previous backend
//! (serde_yaml 0.9.34) before the swap.

fn emit(s: &str) -> String {
    serde_norway::to_string(&s).unwrap()
}

#[test]
fn scalar_rendering_is_stable() {
    let cases: &[(&str, &str)] = &[
        ("", "''\n"),
        ("yes", "yes\n"),
        ("null", "'null'\n"),
        ("~", "'~'\n"),
        ("123", "'123'\n"),
        ("1e3", "'1e3'\n"),
        ("0x1f", "'0x1f'\n"),
        ("a: b", "'a: b'\n"),
        ("- x", "'- x'\n"),
        ("# c", "'# c'\n"),
        ("multi\nline\n", "|\n  multi\n  line\n"),
        ("trail\n\n", "|+\n  trail\n\n"),
        ("  lead", "'  lead'\n"),
        ("tab\t", "\"tab\\t\"\n"),
        ("é日本😀", "é日本😀\n"),
        ("true", "'true'\n"),
        ("2001-01-01", "2001-01-01\n"),
        (".inf", "'.inf'\n"),
        ("@at", "'@at'\n"),
        ("x ", "'x '\n"),
    ];
    for (input, want) in cases {
        assert_eq!(&emit(input), want, "input {input:?}");
    }
}

#[test]
fn scalars_roundtrip() {
    for s in [
        "",
        "yes",
        "null",
        "123",
        "multi\nline\n",
        "trail\n\n",
        "tab\t",
        "é日本😀",
    ] {
        let back: String = serde_norway::from_str(&emit(s)).unwrap();
        assert_eq!(back, s);
    }
}
