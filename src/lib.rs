use std::error::Error;
use std::str::FromStr;

pub mod zfs;

// We use this property to control the retention policy.  Check readme.md, but also
// check_age, ZFS::list_snapshots, and ZFS::list_datasets_for_snapshot.
pub const PROPERTY_SNAPKEEP: &str = "at.rollc.at:snapkeep";

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

// Describes the number of snapshots to keep for each period.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct RetentionPolicy {
    pub yearly: Option<i32>,
    pub monthly: Option<u32>,
    pub weekly: Option<u32>,
    pub daily: Option<u32>,
    pub hourly: Option<u32>,
}

impl RetentionPolicy {
    pub fn rules(&self) -> [(&str, Option<u32>); 5] {
        [
            ("%Y-%m-%d %H", self.hourly),
            ("%Y-%m-%d", self.daily),
            ("%Y w%w", self.weekly),
            ("%Y-%m", self.monthly),
            (
                "%Y",
                // NOTE: chrono keeps years as i32 (signed); however there were no ZFS
                // deployments before ca (+)2006, so I guess it's safe to cast to u32.
                match self.yearly {
                    Some(y) => Some(y as u32),
                    None => None,
                },
            ),
        ]
    }
}

impl FromStr for RetentionPolicy {
    type Err = ();

    fn from_str(x: &str) -> std::result::Result<Self, Self::Err> {
        fn digits_from(start: usize, s: &str) -> &str {
            let s = &s[start..];
            let end = s.chars().take_while(|ch| ch.is_ascii_digit()).count();
            &s[..end]
        }
        let mut policy = RetentionPolicy {
            yearly: None,
            monthly: None,
            weekly: None,
            daily: None,
            hourly: None,
        };
        let mut chars = x.chars().enumerate();
        while let Some((i, ch)) = chars.next() {
            match ch {
                'y' => policy.yearly = digits_from(i + 1, x).parse().ok(),
                'm' => policy.monthly = digits_from(i + 1, x).parse().ok(),
                'w' => policy.weekly = digits_from(i + 1, x).parse().ok(),
                'd' => policy.daily = digits_from(i + 1, x).parse().ok(),
                'h' => policy.hourly = digits_from(i + 1, x).parse().ok(),
                _ => {}
            }
        }

        Ok(policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy_from_str() {
        let actual = RetentionPolicy::from_str("h24d30w8m6y1").unwrap();
        let expected = RetentionPolicy {
            yearly: Some(1),
            monthly: Some(6),
            weekly: Some(8),
            daily: Some(30),
            hourly: Some(24),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_retention_policy_invalid() {
        let actual = RetentionPolicy::from_str("y1d88a1b2c3m5").unwrap();
        let expected = RetentionPolicy {
            yearly: Some(1),
            monthly: Some(5),
            weekly: None,
            daily: Some(88),
            hourly: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_retention_policy_empty() {
        let actual = RetentionPolicy::from_str("").unwrap();
        let expected = RetentionPolicy {
            yearly: None,
            monthly: None,
            weekly: None,
            daily: None,
            hourly: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_retention_policy_truncated() {
        let actual = RetentionPolicy::from_str("y").unwrap();
        let expected = RetentionPolicy {
            yearly: None,
            monthly: None,
            weekly: None,
            daily: None,
            hourly: None,
        };
        assert_eq!(actual, expected);
    }
}
