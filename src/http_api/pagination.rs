use serde::{Deserialize, Serialize};

/// Query parameters for paginated list endpoints.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl PaginationParams {
    /// Default limit when none is specified.
    pub const DEFAULT_LIMIT: i64 = 100;
    /// Maximum allowed limit to prevent abuse.
    pub const MAX_LIMIT: i64 = 1000;

    /// Get the effective limit (default or capped).
    pub fn limit(&self) -> i64 {
        self.limit
            .map(|l| l.clamp(1, Self::MAX_LIMIT))
            .unwrap_or(Self::DEFAULT_LIMIT)
    }

    /// Get the effective offset (defaults to 0).
    pub fn offset(&self) -> i64 {
        self.offset.unwrap_or(0).max(0)
    }
}

/// Paginated response wrapper.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T: Serialize> {
    pub data: Vec<T>,
    pub total: i64,
    pub has_more: bool,
}

impl<T: Serialize> PaginatedResponse<T> {
    /// Create a paginated response from the full dataset.
    ///
    /// Slices `all_data` according to `params` and computes `has_more`.
    pub fn new(all_data: Vec<T>, total: i64, params: &PaginationParams) -> Self {
        let offset = params.offset() as usize;
        let limit = params.limit() as usize;

        let data: Vec<T> = if offset >= all_data.len() {
            Vec::new()
        } else {
            all_data.into_iter().skip(offset).take(limit).collect()
        };

        let has_more = (offset + limit) < total as usize;

        Self {
            data,
            total,
            has_more,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_params_defaults() {
        let params: PaginationParams = serde_json::from_str("{}").unwrap();
        assert_eq!(params.limit(), PaginationParams::DEFAULT_LIMIT);
        assert_eq!(params.offset(), 0);
    }

    #[test]
    fn test_pagination_params_caps_max_limit() {
        let params = PaginationParams {
            limit: Some(9999),
            offset: None,
        };
        assert_eq!(params.limit(), PaginationParams::MAX_LIMIT);
    }

    #[test]
    fn test_pagination_params_clamps_min_limit() {
        let params = PaginationParams {
            limit: Some(0),
            offset: None,
        };
        assert_eq!(params.limit(), 1);
    }

    #[test]
    fn test_paginated_response_has_more_true() {
        let data: Vec<i32> = (0..5).collect();
        let params = PaginationParams {
            limit: Some(3),
            offset: None,
        };
        let resp = PaginatedResponse::new(data, 5, &params);
        assert_eq!(resp.data.len(), 3);
        assert!(resp.has_more);
    }

    #[test]
    fn test_paginated_response_has_more_false() {
        let data: Vec<i32> = (0..3).collect();
        let params = PaginationParams {
            limit: Some(5),
            offset: None,
        };
        let resp = PaginatedResponse::new(data, 3, &params);
        assert_eq!(resp.data.len(), 3);
        assert!(!resp.has_more);
    }

    #[test]
    fn test_paginated_response_with_offset() {
        let data: Vec<i32> = (0..10).collect();
        let params = PaginationParams {
            limit: Some(3),
            offset: Some(5),
        };
        let resp = PaginatedResponse::new(data, 10, &params);
        assert_eq!(resp.data, vec![5, 6, 7]);
        assert!(resp.has_more);
    }

    #[test]
    fn test_paginated_response_offset_past_end() {
        let data: Vec<i32> = (0..5).collect();
        let params = PaginationParams {
            limit: Some(10),
            offset: Some(10),
        };
        let resp = PaginatedResponse::new(data, 5, &params);
        assert!(resp.data.is_empty());
        assert!(!resp.has_more);
    }
}
