use std::marker::PhantomData;

use diesel::mysql::Mysql;
use diesel::pg::Pg;
use diesel::query_dsl::LoadQuery;
use diesel::sql_types::BigInt;
use diesel::{query_builder::*, QueryResult, RunQueryDsl};

pub trait Paginate<DB>: Sized {
    fn paginate(self, offset: usize, limit: usize) -> PaginatedQuery<Self, DB> {
        PaginatedQuery {
            query: self,
            offset,
            limit,
            _marker: PhantomData,
        }
    }
}

impl<T, DB> Paginate<DB> for T {}

#[derive(Debug)]
pub struct PaginatedQuery<T, Conn> {
    query: T,
    offset: usize,
    limit: usize,
    _marker: PhantomData<Conn>,
}

impl<T, Conn> PaginatedQuery<T, Conn> {
    pub fn load_and_total<U>(self, conn: &Conn) -> QueryResult<(Vec<U>, i64)>
    where
        Self: LoadQuery<Conn, (U, i64)>,
    {
        let results = self.internal_load(conn)?;
        let total = *results.get(0).map(|(_, total)| total).unwrap_or(&0);
        let records: Vec<U> = results.into_iter().map(|(record, _)| record).collect();
        Ok((records, total))
    }
}

impl<T, DB> QueryId for PaginatedQuery<T, DB> {
    const HAS_STATIC_QUERY_ID: bool = false;
    type QueryId = ();
}

impl<T: Query, DB> Query for PaginatedQuery<T, DB> {
    type SqlType = (T::SqlType, BigInt);
}

impl<T, Conn> RunQueryDsl<Conn> for PaginatedQuery<T, Conn> {}

impl<T, DB> QueryFragment<Pg> for PaginatedQuery<T, DB>
where
    T: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<'_, Pg>) -> QueryResult<()> {
        out.push_sql("SELECT *, COUNT(*) OVER () FROM (");
        self.query.walk_ast(out.reborrow())?;
        out.push_sql(") t LIMIT ");
        out.push_bind_param::<BigInt, _>(&(self.limit as i64))?;
        out.push_sql(" OFFSET ");
        out.push_bind_param::<BigInt, _>(&(self.offset as i64))?;
        Ok(())
    }
}

impl<T, DB> QueryFragment<Mysql> for PaginatedQuery<T, DB>
where
    T: QueryFragment<Mysql>,
{
    fn walk_ast(&self, mut out: AstPass<'_, Mysql>) -> QueryResult<()> {
        out.push_sql("SELECT *, COUNT(*) OVER () FROM (");
        self.query.walk_ast(out.reborrow())?;
        out.push_sql(") t LIMIT ");
        out.push_bind_param::<BigInt, _>(&(self.limit as i64))?;
        out.push_sql(" OFFSET ");
        out.push_bind_param::<BigInt, _>(&(self.offset as i64))?;
        Ok(())
    }
}
