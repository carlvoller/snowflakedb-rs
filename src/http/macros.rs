macro_rules! params {
    ( $( ($key:expr, $val:expr) ),* $(,)? ) => {{
        let mut v = Vec::new();
        trait AsParam {
            fn as_param(&self) -> Option<String>;
        }
        impl AsParam for &str {
            fn as_param(&self) -> Option<String> { Some(self.to_string()) }
        }

        impl AsParam for Option<String> {
            fn as_param(&self) -> Option<String> { self.clone() }
        }

        impl AsParam for Option<&str> {
            fn as_param(&self) -> Option<String> { self.map(|x| x.to_string()) }
        }

        impl AsParam for String {
            fn as_param(&self) -> Option<String> { Some(self.clone()) }
        }
        $(
            if let Some(s) = AsParam::as_param(&$val) {
                v.push(($key.to_string(), s));
            }
        )*
        v
    }};
}

pub(crate) use params;
