use super::{format_file_size, format_timestamp};

#[test]
fn format_file_size_uses_expected_units() {
    assert_eq!(format_file_size(512), "512 B");
    assert_eq!(format_file_size(1024), "1.00 KB");
    assert_eq!(format_file_size(1024 * 1024), "1.00 MB");
    assert_eq!(format_file_size(1024 * 1024 * 1024), "1.00 GB");
}

#[test]
fn format_timestamp_handles_regular_and_out_of_range_values() {
    assert!(format_timestamp(0).starts_with("1970-01-01"));
    assert_eq!(format_timestamp(i64::MAX), "-");
}
