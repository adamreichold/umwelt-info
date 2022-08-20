use std::time::{Duration, SystemTime};

use askama::Result;
use time::{macros::format_description, OffsetDateTime};

pub fn system_time(val: &SystemTime) -> Result<String> {
    let val = OffsetDateTime::from(*val)
        .format(format_description!("[day].[month].[year] [hour]:[minute]"))
        .unwrap();

    Ok(val)
}

pub fn duration(val: &Duration) -> Result<String> {
    let secs = val.as_secs();

    let val = if secs > 3600 {
        format!("{}h", secs / 3600)
    } else if secs > 60 {
        format!("{}min", secs / 60)
    } else {
        format!("{}s", secs)
    };

    Ok(val)
}

pub fn percentage(val: &f64) -> Result<String> {
    Ok(format!("{:.0} %", 100.0 * val))
}
