//! Conversions between the integer types that limits and byte counts come in.

// `count` as `usize`, or `usize::MAX` if it doesn't fit.
pub(crate) fn saturating_usize<T>(count: T) -> usize
where
    usize: TryFrom<T>,
{
    usize::try_from(count).unwrap_or(usize::MAX)
}

// `count` as `u64`, or `u64::MAX` if it doesn't fit.
pub(crate) fn saturating_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_that_fit_are_kept() {
        assert_eq!(saturating_usize(7u64), 7);
        assert_eq!(saturating_u64(7), 7);
    }

    #[test]
    fn count_past_the_target_is_capped() {
        assert_eq!(saturating_usize(u128::MAX), usize::MAX);
    }
}
