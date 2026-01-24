use std::error::Error;
use std::fmt::{Debug, Display};

#[derive(Clone)]
pub struct SnowflakeError {
    trace: String,
    message: String,
    underlying_error: Option<String>,
}

impl SnowflakeError {
    pub(crate) fn new(trace: String, message: String, underlying: Option<String>) -> Self {
        Self {
            trace,
            message,
            underlying_error: underlying,
        }
    }
}

impl Error for SnowflakeError {}

impl Display for SnowflakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error_name = self.underlying_error.as_deref().unwrap_or("SnowflakeError");
        write!(f, "[{}] ({}): {}", error_name, self.trace, self.message)
    }
}

impl Debug for SnowflakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error_name = self.underlying_error.as_deref().unwrap_or("SnowflakeError");
        write!(f, "[{}] ({}): {}", error_name, self.trace, self.message)
    }
}

macro_rules! this_errors {
    ($msg:literal, $val:expr) => {
        $val.map_err(|e| $crate::error!($msg, e))?
    };
}

macro_rules! error {
    ($val:literal) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        let fun_name = &name[..name.len() - 3];
        $crate::errors::SnowflakeError::new(fun_name.into(), $val.into(), None)
    }};
    ($err:expr) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let err = $err;
        let name = type_name_of(f);
        let error_type_name = type_name_of(&err);
        let fun_name = &name[..name.len() - 3];
        let error_name = error_type_name.split("::").last().map(|x| x.to_string());
        let error_msg = format!("{:?}", err);

        $crate::errors::SnowflakeError::new(fun_name.into(), error_msg, error_name)
    }};
    ($val:literal, $err:expr) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let err = $err;
        let name = type_name_of(f);
        let error_type_name = type_name_of(&err);
        let fun_name = &name[..name.len() - 3];
        let error_name = error_type_name.split("::").last().map(|x| x.to_string());
        let final_msg = format!("{:?} - {:?}", $val, err);
        $crate::errors::SnowflakeError::new(fun_name.into(), final_msg, error_name)
    }};
}

pub(crate) use error;
pub(crate) use this_errors;
