use crate::{auth::session::Session, driver::{Protocol, base::Bindings}};

pub struct ArrowProtocol {}

impl Protocol for ArrowProtocol {
    type Query = ArrowQuery;
}

impl Default for ArrowProtocol {
    fn default() -> Self {
        Self {}
    }
}

pub struct ArrowQuery {
    session: Session,
    bindings: Bindings,
    query: String,
}