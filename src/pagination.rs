pub use avantis_utils_macro::PaginatedQuery;

pub trait PaginatedQuery {
    fn limit(&self) -> i32;
    fn offset(&self) -> i32;
}

mod tests {
    use super::*;

    #[test]
    fn test_input() {
        #[derive(Default, Debug, PartialEq, PaginatedQuery)]
        struct Input {
            #[limit(default = 100)]
            pub limit_t: Option<i32>,
            #[offset(default = 0)]
            pub offset_t: Option<i32>,
        }

        let input = Input::default();

        assert_eq!(100, input.limit());
        assert_eq!(0, input.offset());
    }
}
